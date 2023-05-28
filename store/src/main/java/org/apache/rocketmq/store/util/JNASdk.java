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

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
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
     * @param address a address with Pointer type
     * @param size lock length, unit is byte
     * @return
     */
    public static int mlock(Pointer address, long size) {
        boolean locked = false;
        if (Platform.isWindows()) {
            LibKernel32.MemoryBasicInformation memInfo = new LibKernel32.MemoryBasicInformation();
            if (LibKernel32.INSTANCE.VirtualQueryEx(WINDOWS_PROCESS_HANDLE, address, memInfo, memInfo.size()) != 0) {
                locked = LibKernel32.INSTANCE.VirtualLock(memInfo.baseAddress, Math.max(memInfo.regionSize, size));
            }
        } else {
            locked = LibC.INSTANCE.mlock(address, new NativeLong(size)) == 0;
        }
        if (!locked) {
            log.warn("mlock memory fail, Error code {}", Native.getLastError());
        }

        return locked ? 0 : -1;
    }

    /**
     * memory unlock
     *
     * @param address a address with Pointer type
     * @param size unlock length, unit is byte
     * @return success is 1, fail is -1
     */
    public static int munlock(Pointer address, long size) {
        boolean unlocked = false;
        if (Platform.isWindows()) {
            LibKernel32.MemoryBasicInformation memInfo = new LibKernel32.MemoryBasicInformation();
            if (LibKernel32.INSTANCE.VirtualQueryEx(WINDOWS_PROCESS_HANDLE, address, memInfo, memInfo.size()) != 0) {
                unlocked = LibKernel32.INSTANCE.VirtualUnlock(memInfo.baseAddress, Math.max(memInfo.regionSize, size));
            }
        } else {
            unlocked = LibC.INSTANCE.munlock(address, new NativeLong(size)) == 0;
        }
        if (!unlocked) {
            log.warn("Unlock memory fail, Error code {}", Native.getLastError());
        }
        return unlocked ? 0 : -1;
    }

    public static int madvise(Pointer pointer, long size) {
        if (Platform.isWindows()) {

        } else {
            return LibC.INSTANCE.madvise(pointer, new NativeLong(size), LibC.MADV_WILLNEED);
        }
        return 0;
    }

    public static LibKernel32 getLibKernel32() {
        if (Platform.isWindows()) {
            return LibKernel32.INSTANCE;
        }
        return null;
    }
}
