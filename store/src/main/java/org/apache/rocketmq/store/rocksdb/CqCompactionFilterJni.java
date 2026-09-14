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
    private static final Object FILTER_LOCK = new Object();
    private static volatile boolean loaded = false;

    /** Platform-specific shim library name and extension. */
    private static final String SHIM_LIB_NAME;
    private static final String SHIM_LIB_EXTENSION;

    static {
        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch");
        if (os.contains("mac") || os.contains("darwin") || os.contains("osx")) {
            SHIM_LIB_NAME = "libcq_compaction_filter.dylib";
            SHIM_LIB_EXTENSION = ".dylib";
        } else if (os.contains("win")) {
            SHIM_LIB_NAME = "cq_compaction_filter.dll";
            SHIM_LIB_EXTENSION = ".dll";
        } else {
            SHIM_LIB_NAME = arch.contains("aarch") || arch.contains("arm")
                ? "libcq_compaction_filter_aarch64.so"
                : "libcq_compaction_filter.so";
            SHIM_LIB_EXTENSION = ".so";
        }
    }

    static {
        loadNativeShim();
    }

    private static synchronized void loadNativeShim() {
        if (loaded) {
            return;
        }

        String libName = SHIM_LIB_NAME;
        try (InputStream is = CqCompactionFilterJni.class
            .getClassLoader().getResourceAsStream("native/" + libName)) {
            if (is == null) {
                log.error("[CqCompactionFilterJni] Native library '{}' not found on classpath", libName);
                return;
            }
            File tempLib = File.createTempFile("cq_compaction_filter_", SHIM_LIB_EXTENSION);
            Files.copy(is, tempLib.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempLib.deleteOnExit();
            System.load(tempLib.getAbsolutePath());
            loaded = true;
            log.info("[CqCompactionFilterJni] Native library loaded from classpath: {}", tempLib.getAbsolutePath());
        } catch (IOException e) {
            log.error("[CqCompactionFilterJni] Failed to load native shim", e);
        } catch (UnsatisfiedLinkError e) {
            log.error("[CqCompactionFilterJni] Failed to link native shim", e);
        }
    }

    /**
     * Returns whether the native compaction filter shim was successfully loaded.
     */
    public static boolean isLoaded() {
        return loaded;
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
     * Delete the native CqCompactionFilter instance.
     * Must only be called after all compaction threads have stopped.
     */
    public static native void destroyNativeFilter0(long filterPtr);

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
        synchronized (FILTER_LOCK) {
            long oldPtr = NATIVE_FILTER_PTR.getAndSet(0);
            if (oldPtr != 0) {
                destroyNativeFilter0(oldPtr);
                log.warn("CqCompactionFilter replaced without explicit destroy");
            }
            long ptr = createNativeFilter0();
            NATIVE_FILTER_PTR.set(ptr);
            setNativeFilter(options, ptr);
            return ptr;
        }
    }

    /**
     * Update the minPhyOffset on the current native filter.
     */
    public static void setMinPhyOffset(long minPhyOffset) {
        synchronized (FILTER_LOCK) {
            long ptr = NATIVE_FILTER_PTR.get();
            if (ptr != 0) {
                setMinPhyOffset0(ptr, minPhyOffset);
                log.info("CqCompactionFilter setMinPhyOffset={}", minPhyOffset);
            }
        }
    }

    /**
     * Destroy the native filter. Must be called only after
     * cancelAllBackgroundWork(true) ensures no compaction thread is running.
     */
    public static void destroyNativeFilter() {
        synchronized (FILTER_LOCK) {
            long ptr = NATIVE_FILTER_PTR.getAndSet(0);
            if (ptr != 0) {
                destroyNativeFilter0(ptr);
                log.info("CqCompactionFilter destroyed");
            }
        }
    }
}