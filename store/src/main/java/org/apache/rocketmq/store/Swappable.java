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
package org.apache.rocketmq.store;

/**
 * Swapping mapped-byte-buffer to save memory on a system with extra-large disks.
 *
 * Reason:
 * Mapping items between virtual memory and physical memory occupies memory space. If more mapped-memory is used and not
 * freed, page table gets bigger and bigger, which can cause physical memory shortages.
 *
 * Solution:
 * Memory in the page table can be partially freed if we periodically clean up the mmap of "cold data".
 *
 */
public interface Swappable {
    /**
     * Swap a mapped but un-referred mapped buffer.
     *
     * @param reserveNum keep the reserved number of files from being swapped.
     * @param forceSwapIntervalMs the force interval of swapping buffer
     * @param normalSwapIntervalMs  the normal interval of swapping buffer
     */
    void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs);

    /**
     * Clean the swapped buffer.
     *
     * @param forceCleanSwapIntervalMs the force interval of cleaning swapped buffer
     */
    void cleanSwappedMap(long forceCleanSwapIntervalMs);
}
