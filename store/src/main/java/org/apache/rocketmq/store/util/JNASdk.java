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
package org.apache.rocketmq.store.util;

import com.sun.jna.LastErrorException;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.StoreUtil;

public class JNASdk {
    protected static final Logger log = LoggerFactory.getLogger(JNASdk.class);

    final static Pointer WINDOWS_PROCESS_HANDLE;

    static {
        if (Platform.isWindows()) {
            WINDOWS_PROCESS_HANDLE = LibKernel32.INSTANCE.GetCurrentProcess();

            // cause by Windows limits the number of pages that can be locked
            // init, set can lock memory size,
            if (!LibKernel32.INSTANCE.SetProcessWorkingSetSize(WINDOWS_PROCESS_HANDLE, StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE / 10 * 8, StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE)) {
                log.warn("Unable to set lock memory size, Error code {}", Native.getLastError());
            }
        } else {
            WINDOWS_PROCESS_HANDLE = null;
        }
    }

    /**
     * memory lock
     *
     * @param address A memory address with Pointer type
     * @param size lock length, unit is byte
     * @return
     */
    public static boolean mlock(Pointer address, long size) {
        boolean locked = false;
        try {
            if (Platform.isWindows()) {
                LibKernel32.MemoryBasicInformation memInfo = new LibKernel32.MemoryBasicInformation();
                if (LibKernel32.INSTANCE.VirtualQueryEx(WINDOWS_PROCESS_HANDLE, address, memInfo, memInfo.size()) != 0) {
                    locked = LibKernel32.INSTANCE.VirtualLock(memInfo.baseAddress, Math.min(memInfo.regionSize, size));
                }
            } else {
                locked = LibC.INSTANCE.mlock(address, new NativeLong(size)) == 0;
            }
            if (!locked) {
                int errno = Native.getLastError();
                log.warn("mlock memory fail, Err code {}, Error string {}",
                        errno, getErrString(errno));
            }
        } catch (UnsatisfiedLinkError e) {
            log.error("Unable to load dynamic library", e);
        }
        return locked;
    }

    /**
     * memory unlock
     *
     * @param address A memory address with Pointer type
     * @param size unlock length, unit is byte
     * @return success is 1, fail is -1
     */
    public static boolean munlock(Pointer address, long size) {
        boolean unlocked = false;
        try {
            if (Platform.isWindows()) {
                LibKernel32.MemoryBasicInformation memInfo = new LibKernel32.MemoryBasicInformation();
                if (LibKernel32.INSTANCE.VirtualQueryEx(WINDOWS_PROCESS_HANDLE, address, memInfo, memInfo.size()) != 0) {
                    unlocked = LibKernel32.INSTANCE.VirtualUnlock(memInfo.baseAddress, Math.min(memInfo.regionSize, size));
                }
            } else {
                unlocked = LibC.INSTANCE.munlock(address, new NativeLong(size)) == 0;
            }
            if (!unlocked) {
                int errno = Native.getLastError();
                log.warn("munlock memory fail, Err code {}, Error string {}",
                        errno, getErrString(errno));
            }
        } catch (UnsatisfiedLinkError e) {
            log.error("Unable to load dynamic library", e);
        }
        return unlocked;
    }

    public static boolean madvise(Pointer pointer, long size) {
        boolean advised = false;
        try {
            if (Platform.isWindows()) {
                return false;
            } else {
                advised = LibC.INSTANCE.madvise(pointer, new NativeLong(size), LibC.MADV_WILLNEED) == 0;
            }
            if (!advised) {
                int errno = Native.getLastError();
                log.warn("madvise memory fail, Err code {}, Error string {}",
                        errno, getErrString(errno));
            }
        } catch (UnsatisfiedLinkError e) {
            log.error("Unable to load dynamic library", e);
        }
        return advised;
    }

    /**
     * Determine whether virtual pages reside in memory, Only supports Linux/Unix
     *
     * @param address A memory address with Pointer type
     * @param pageLength Page length starting from base address
     * @param pageCacheRst The array length should be param.pageSize/os.pageSize,
     *                    Each index bit indicates whether the corresponding virtual page is in physical memory
     * @return
     */
    public static boolean mincore(Pointer address, long pageLength, byte[] pageCacheRst) {
        if (!Platform.isWindows()) {
            return LibC.INSTANCE.mincore(address, new NativeLong(pageLength), pageCacheRst) == 0;
        }
        return false;
    }

    /**
     * Returns the size of a page, unit is byte, Only supports Linux/Unix
     * This value is the os paging size and may not necessarily be the same as the hardware paging size
     * @return virtual page size
     */
    public static int getpagesize() {
        if (!Platform.isWindows()) {
            return LibC.INSTANCE.getpagesize();
        }
        return 0;
    }

    public static LibKernel32 getLibKernel32() {
        if (Platform.isWindows()) {
            return LibKernel32.INSTANCE;
        }
        return null;
    }

    private static String getErrString(int errno) {
        if (Platform.isWindows()) {
            Memory memory = new Memory(128);
            try {
                PointerByReference buffer = new PointerByReference(memory);
                int nLen = LibKernel32.INSTANCE.FormatMessage(
                        LibKernel32.FORMAT_MESSAGE_ALLOCATE_BUFFER
                                | LibKernel32.FORMAT_MESSAGE_FROM_SYSTEM
                                | LibKernel32.FORMAT_MESSAGE_IGNORE_INSERTS,
                        null,
                        errno,
                        0,
                        buffer, 1024, null);
                if (nLen == 0) {
                    throw new LastErrorException(Native.getLastError());
                }
                Pointer ptr = buffer.getValue();
                String s = ptr.getWideString(0);
                return s.trim();
            } finally {
                memory.clear();
            }
        } else {
            if (errno == LibC.RLIMIT_MEMLOCK_ERRNO) {
                return "System parameter RLIMIT_MEMLOCK soft resource limit";
            }
            return LibC.INSTANCE.strerror(errno);
        }
    }
}
