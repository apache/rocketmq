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
package org.apache.rocketmq.tieredstore.common;

import java.util.concurrent.atomic.LongAdder;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class SelectMappedBufferResultWrapper {

    private final SelectMappedBufferResult result;
    private final LongAdder accessCount;

    private final long curOffset;
    private final long minOffset;
    private final long maxOffset;
    private final long size;

    public SelectMappedBufferResultWrapper(
        SelectMappedBufferResult result, long curOffset, long minOffset, long maxOffset, long size) {

        this.result = result;
        this.accessCount = new LongAdder();
        this.curOffset = curOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.size = size;
    }

    public SelectMappedBufferResult getResult() {
        return result;
    }

    public SelectMappedBufferResult getDuplicateResult() {

        return new SelectMappedBufferResult(
            result.getStartOffset(),
            result.getByteBuffer().asReadOnlyBuffer(),
            result.getSize(),
            result.getMappedFile());
    }

    public long getCurOffset() {
        return curOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public long getSize() {
        return size;
    }

    public void addAccessCount() {
        accessCount.increment();
    }

    public long getAccessCount() {
        return accessCount.sum();
    }
}
