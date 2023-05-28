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
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.win32.StdCallLibrary;

interface LibKernel32 extends StdCallLibrary {

    LibKernel32 INSTANCE = (LibKernel32) Native.loadLibrary("Kernel32", LibKernel32.class);

    /**
     * Locks the specified region of the process's virtual address space into physical
     * memory, ensuring that subsequent access to the region will not incur a page fault.
     *
     * https://learn.microsoft.com/zh-cn/windows/win32/api/memoryapi/nf-memoryapi-virtuallock
     *
     * @param address A pointer to the base address of the region of pages to be locked.
     * @param size The size of the region to be locked, in bytes.
     * @return true if the function succeeds
     */
    boolean VirtualLock(Pointer address, long size);

    /**
     * UnLocks the specified region of the process's virtual address space into physical
     * memory, ensuring that subsequent access to the region will not incur a page fault.
     *
     * Warn, Calling VirtualUnlock on a range of memory that is not locked releases the pages from the process's working set.
     *
     * https://learn.microsoft.com/zh-cn/windows/win32/api/memoryapi/nf-memoryapi-virtuallock
     *
     * @param address A pointer to the base address of the region of pages to be unlock.
     * @param size The size of the region to be unlock, in bytes.
     * @return true if the function succeeds
     */
    boolean VirtualUnlock(Pointer address, long size);

    /**
     * Retrieves a pseudo handle for the current process.
     *
     * https://learn.microsoft.com/zh-cn/windows/win32/api/processthreadsapi/nf-processthreadsapi-getcurrentprocess
     *
     * @return a pseudo handle to the current process.
     */
    Pointer GetCurrentProcess();

    /**
     * Sets the minimum and maximum working set sizes for the specified process.
     *
     * https://learn.microsoft.com/zh-cn/windows/win32/api/memoryapi/nf-memoryapi-setprocessworkingsetsize
     *
     * @param handle A handle to the process whose working set sizes is to be set.
     * @param minSize The minimum working set size for the process, in bytes.
     * @param maxSize The maximum working set size for the process, in bytes.
     * @return true if the function succeeds.
     */
    boolean SetProcessWorkingSetSize(Pointer handle, long minSize, long maxSize);

    /**
     * Retrieves information about a range of pages within the virtual address space of a specified process.
     *
     * https://learn.microsoft.com/zh-cn/windows/win32/api/memoryapi/nf-memoryapi-virtualqueryex
     *
     * @param handle A handle to the process whose memory information is queried.
     * @param address A pointer to the base address of the region of pages to be queried.
     * @param structure A pointer to a structure in which information about the specified page range is returned.
     * @param length The size of the buffer pointed to by the memoryInfo parameter, in bytes.
     * @return the actual number of bytes returned in the information buffer.
     */
    int VirtualQueryEx(Pointer handle, Pointer address, MemoryBasicInformation structure, int length);

    @Structure.FieldOrder({"baseAddress", "allocationBase", "allocationProtect", "regionSize", "state", "protect", "type"})
    class MemoryBasicInformation extends Structure {
        public Pointer baseAddress;
        public Pointer allocationBase;
        public NativeLong allocationProtect;
        public long regionSize;
        public NativeLong state;
        public NativeLong protect;
        public NativeLong type;
    }
}
