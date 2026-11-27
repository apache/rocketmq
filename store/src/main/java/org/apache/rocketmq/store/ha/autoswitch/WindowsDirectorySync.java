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

package org.apache.rocketmq.store.ha.autoswitch;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.PointerType;
import com.sun.jna.WString;
import com.sun.jna.win32.StdCallLibrary;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class WindowsDirectorySync {
    private static final int GENERIC_WRITE = 0x40000000;
    private static final int SHARE_READ_WRITE_DELETE = 7;
    private static final int OPEN_EXISTING = 3;
    private static final int FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;

    private WindowsDirectorySync() {
    }

    public static class Handle extends PointerType {
        boolean isInvalid() {
            long value = Pointer.nativeValue(getPointer());
            return value == -1 || Native.POINTER_SIZE == 4 && value == 0xffffffffL;
        }
    }

    public interface Kernel32 extends StdCallLibrary {
        Handle CreateFileW(WString name, int access, int sharing, Pointer security, int disposition, int flags, Pointer template);

        boolean FlushFileBuffers(Handle handle);

        boolean CloseHandle(Handle handle);

        int GetLastError();
    }

    private static final class NativeKernel {
        static final Kernel32 INSTANCE = (Kernel32) Native.loadLibrary("kernel32", Kernel32.class);
    }

    static void force(Path directory) throws IOException {
        Path resolved = directory.toRealPath();
        String name = resolved.toString();
        if (!name.startsWith("\\\\?\\")) {
            name = name.startsWith("\\\\") ? "\\\\?\\UNC\\" + name.substring(2) : "\\\\?\\" + name;
        }
        try {
            force(new WString(name), Files.getFileStore(resolved).type(), NativeKernel.INSTANCE);
        } catch (LinkageError unavailable) {
            throw new IOException("Cannot load Windows directory flush support for " + directory, unavailable);
        }
    }

    static void force(WString name, String fileSystem, Kernel32 kernel) throws IOException {
        // Some Windows file systems accept a directory flush without flushing it (MS-FSA, note 80).
        if (!"NTFS".equalsIgnoreCase(fileSystem)) {
            throw new IOException("HA history requires NTFS directory flush support at " + name + ", found " + fileSystem);
        }
        Handle handle = kernel.CreateFileW(name, GENERIC_WRITE, SHARE_READ_WRITE_DELETE, null, OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS, null);
        if (handle == null || handle.isInvalid()) {
            throw failure("CreateFileW", name, kernel.GetLastError());
        }
        try (Closeable closer = () -> {
            if (!kernel.CloseHandle(handle)) {
                throw failure("CloseHandle", name, kernel.GetLastError());
            }
        }) {
            if (!kernel.FlushFileBuffers(handle)) {
                throw failure("FlushFileBuffers", name, kernel.GetLastError());
            }
        }
    }

    private static IOException failure(String operation, WString name, int error) {
        return new IOException(operation + " failed for directory " + name + " with Windows error " + error);
    }
}
