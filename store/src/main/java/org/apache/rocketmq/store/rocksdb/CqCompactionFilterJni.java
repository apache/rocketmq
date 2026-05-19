/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.rocksdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.rocksdb.ColumnFamilyOptions;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class CqCompactionFilterJni {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);

    private static final AtomicLong NATIVE_FILTER_PTR = new AtomicLong(0);
    private static volatile boolean loaded = false;

    /** Platform-specific shim library name and extension. */
    private static final String SHIM_LIB_NAME;
    private static final String SHIM_LIB_EXTENSION;
    private static final String ROCKSDB_JNI_LIB_NAME;

    static {
        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch");
        if (os.contains("mac") || os.contains("darwin") || os.contains("osx")) {
            SHIM_LIB_NAME = "libcq_compaction_filter.dylib";
            SHIM_LIB_EXTENSION = ".dylib";
            ROCKSDB_JNI_LIB_NAME = arch.contains("aarch") || arch.contains("arm")
                ? "librocksdbjni-osx-aarch64"
                : "librocksdbjni-osx-x86_64";
        } else if (os.contains("win")) {
            SHIM_LIB_NAME = "cq_compaction_filter.dll";
            SHIM_LIB_EXTENSION = ".dll";
            ROCKSDB_JNI_LIB_NAME = "librocksdbjni-win64.dll";
        } else {
            SHIM_LIB_NAME = arch.contains("aarch") || arch.contains("arm")
                ? "libcq_compaction_filter_aarch64.so"
                : "libcq_compaction_filter.so";
            SHIM_LIB_EXTENSION = ".so";
            ROCKSDB_JNI_LIB_NAME = arch.contains("aarch") || arch.contains("arm")
                ? "librocksdbjni-linux-aarch64.so"
                : "librocksdbjni-linux64.so";
        }
    }

    static {
        loadNativeShim();
    }

    private static synchronized void loadNativeShim() {
        if (loaded) {
            return;
        }

        // Preload RocksDB's native library so that linked symbols are available
        // when our compaction filter shim is loaded.
        String rocksdbDir = ensureRocksDBNativeLoaded();

        String libName = SHIM_LIB_NAME;
        try (InputStream is = CqCompactionFilterJni.class
            .getClassLoader().getResourceAsStream("native/" + libName)) {
            if (is == null) {
                log.error("[CqCompactionFilterJni] Native library '{}' not found on classpath", libName);
                return;
            }
            File tempLib;
            if (rocksdbDir != null) {
                // Extract our shim to the same temp directory as the RocksDB JNI library,
                // so that the DT_NEEDED / LC_LOAD_DYLIB dependency can be resolved.
                tempLib = new File(rocksdbDir, libName);
            } else {
                // RocksDB was loaded from java.library.path; our shim can go anywhere.
                tempLib = File.createTempFile("cq_compaction_filter_", SHIM_LIB_EXTENSION);
            }
            Files.copy(is, tempLib.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempLib.deleteOnExit();
            System.load(tempLib.getAbsolutePath());
            loaded = true;
            log.info("[CqCompactionFilterJni] Native library loaded from classpath: {}", tempLib.getAbsolutePath());
        } catch (IOException e) {
            log.error("[CqCompactionFilterJni] Failed to load native shim", e);
        }
    }

    /**
     * Returns whether the native compaction filter shim was successfully loaded.
     */
    public static boolean isLoaded() {
        return loaded;
    }

    /**
     * Locates and loads the RocksDB native JNI library, returning the temporary
     * directory in which it was extracted (or null if loaded from java.library.path).
     * <p>
     * This method deliberately uses {@code System.loadLibrary("rocksdbjni")}
     * rather than {@code RocksDB.loadLibrary()} for the following reasons:
     * <ol>
     * <li><b>Avoid unnecessary side effects</b> — {@code RocksDB.loadLibrary()}
     *     iterates over all compression types (snappy, lz4, zstd, bzip2, etc.)
     *     and attempts to load each one. Those libraries are not needed by this
     *     compaction filter, and the resulting {@code UnsatisfiedLinkError}s slow
     *     down startup and pollute logs.</li>
     * <li><b>Control the temp directory location</b> — The caller needs to know
     *     the directory where the native JNI library was extracted so that
     *     {@code libcq_compaction_filter.so} can be placed alongside it. This is
     *     required for the dynamic linker to resolve the {@code DT_NEEDED}
     *     dependency of the custom shim. {@code RocksDB.loadLibrary()} extracts
     *     to an internal temp directory that is not exposed to callers.</li>
     * <li><b>Avoid class-loading coupling</b> — {@code RocksDB.loadLibrary()}
     *     triggers the full initialization chain of the rocksdbjni Java bindings
     *     (including {@code CompressionType.values()} iteration and a singleton
     *     {@code NativeLibraryLoader} state machine). Loading the custom shim
     *     must complete before any RocksDB Java classes are exercised, to avoid
     *     native symbol resolution race conditions.</li>
     * </ol>
     *
     * @return the absolute path of the temporary directory containing the
     *         extracted RocksDB JNI library, or null if the library was loaded
     *         from {@code java.library.path} (in which case no temp directory
     *         is needed for the shim).
     */
    private static String ensureRocksDBNativeLoaded() {
        // Try System.loadLibrary first (works if on java.library.path)
        try {
            System.loadLibrary("rocksdbjni");
            // No temp dir needed since it's on java.library.path
            return null;
        } catch (UnsatisfiedLinkError ignored) {
            // Not on java.library.path, try from JAR
        }

        // Determine the platform-specific JNI library name from RocksDB's Environment
        String jniLibName;
        try {
            jniLibName = org.rocksdb.util.Environment.getJniLibraryFileName("rocksdb");
        } catch (Exception e) {
            jniLibName = ROCKSDB_JNI_LIB_NAME;
        }

        try (InputStream is = CqCompactionFilterJni.class.getClassLoader().getResourceAsStream(jniLibName)) {
            if (is == null) {
                log.error("[CqCompactionFilterJni] RocksDB native library '{}' not found on classpath", jniLibName);
                return null;
            }
            // Create a temp directory and extract the library there.
            // Our shim will be placed in the same directory so the DT_NEEDED
            // dependency resolves correctly.
            File tempDir = Files.createTempDirectory("rocksdb-native").toFile();
            tempDir.deleteOnExit();
            File tempLib = new File(tempDir, jniLibName);
            Files.copy(is, tempLib.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempLib.deleteOnExit();
            System.load(tempLib.getAbsolutePath());
            return tempDir.getAbsolutePath();
        } catch (IOException e) {
            log.error("[CqCompactionFilterJni] Failed to extract RocksDB native library", e);
            return null;
        }
    }

    /**
     * Create a native CqCompactionFilter instance.
     * Returns the raw C++ pointer as a jlong.
     */
    public static native long createNativeFilter0();

    /**
     * Update the minPhyOffset threshold on an existing native filter.
     */
    public static native void setMinPhyOffset0(long filterPtr, long minPhyOffset);

    /**
     * Set the native compaction filter on the ColumnFamilyOptions via the
     * public {@code setCompactionFilter} API.
     * <p>
     * The wrapper uses {@code disOwnNativeHandle()} so that closing the
     * ColumnFamilyOptions does not free the native filter — this prevents
     * use-after-free when AbstractRocksDBStorage closes options before the DB.
     */
    public static void setNativeFilter(ColumnFamilyOptions options, long filterPtr) {
        NativeCqCompactionFilter filter = new NativeCqCompactionFilter(filterPtr);
        options.setCompactionFilter(filter);
    }

    /**
     * Create the native filter and set it on the ColumnFamilyOptions.
     * Returns the native pointer for later threshold updates.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static long createAndSetFilter(ColumnFamilyOptions options) {
        long ptr = createNativeFilter0();
        NATIVE_FILTER_PTR.set(ptr);
        setNativeFilter(options, ptr);
        return ptr;
    }

    /**
     * Update the minPhyOffset on the current native filter.
     */
    public static void setMinPhyOffset(long minPhyOffset) {
        long ptr = NATIVE_FILTER_PTR.get();
        if (ptr != 0) {
            setMinPhyOffset0(ptr, minPhyOffset);
            log.info("CqCompactionFilter setMinPhyOffset={}", minPhyOffset);
        }
    }
}