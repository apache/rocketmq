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
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIOptions;

interface LibKernel32 extends StdCallLibrary {

    LibKernel32 INSTANCE = (LibKernel32) Native.loadLibrary("Kernel32", LibKernel32.class, W32APIOptions.DEFAULT_OPTIONS);

    /**
     * The lpBuffer parameter is a pointer to a PVOID pointer, and that the nSize
     * parameter specifies the minimum number of TCHARs to allocate for an output
     * message buffer. The function allocates a buffer large enough to hold the
     * formatted message, and places a pointer to the allocated buffer at the address
     * specified by lpBuffer. The caller should use the LocalFree function to free
     * the buffer when it is no longer needed.
     */
    int FORMAT_MESSAGE_ALLOCATE_BUFFER = 0x00000100;

    /**
     * Insert sequences in the message definition are to be ignored and passed through
     * to the output buffer unchanged. This flag is useful for fetching a message for
     * later formatting. If this flag is set, the Arguments parameter is ignored.
     */
    int FORMAT_MESSAGE_IGNORE_INSERTS  = 0x00000200;

    /**
     * The lpSource parameter is a pointer to a null-terminated message definition.
     * The message definition may contain insert sequences, just as the message text
     * in a message table resource may. Cannot be used with FORMAT_MESSAGE_FROM_HMODULE
     * or FORMAT_MESSAGE_FROM_SYSTEM.
     */
    int FORMAT_MESSAGE_FROM_STRING     = 0x00000400;

    /**
     * The lpSource parameter is a module handle containing the message-table
     * resource(s) to search. If this lpSource handle is NULL, the current process's
     * application image file will be searched. Cannot be used with
     * FORMAT_MESSAGE_FROM_STRING.
     */
    int FORMAT_MESSAGE_FROM_HMODULE    = 0x00000800;

    /**
     * The function should search the system message-table resource(s) for the
     * requested message. If this flag is specified with FORMAT_MESSAGE_FROM_HMODULE,
     * the function searches the system message table if the message is not found in
     * the module specified by lpSource. Cannot be used with FORMAT_MESSAGE_FROM_STRING.
     * If this flag is specified, an application can pass the result of the
     * GetLastError function to retrieve the message text for a system-defined error.
     */
    int FORMAT_MESSAGE_FROM_SYSTEM     = 0x00001000;

    /**
     * The Arguments parameter is not a va_list structure, but is a pointer to an array
     * of values that represent the arguments. This flag cannot be used with 64-bit
     * argument values. If you are using 64-bit values, you must use the va_list
     * structure.
     */
    int FORMAT_MESSAGE_ARGUMENT_ARRAY  = 0x00002000;

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

    int FormatMessage(int dwFlags, Pointer lpSource, int dwMessageId,
                      int dwLanguageId, PointerByReference lpBuffer, int nSize,
                      Pointer vaList);

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
