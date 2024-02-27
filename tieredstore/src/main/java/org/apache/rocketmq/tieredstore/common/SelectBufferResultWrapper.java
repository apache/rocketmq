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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class SelectBufferResultWrapper {

    private final SelectMappedBufferResult result;
    private final long offset;
    private final long tagCode;
    private final AtomicInteger accessCount;

    public SelectBufferResultWrapper(SelectMappedBufferResult result, long offset, long tagCode, boolean used) {
        this.result = result;
        this.offset = offset;
        this.tagCode = tagCode;
        this.accessCount = new AtomicInteger(used ? 1 : 0);
    }

    public SelectMappedBufferResult getDuplicateResult() {

        return new SelectMappedBufferResult(
            result.getStartOffset(),
            result.getByteBuffer().asReadOnlyBuffer(),
            result.getSize(),
            result.getMappedFile());
    }

    public long getOffset() {
        return offset;
    }

    public int getBufferSize() {
        return this.result.getSize();
    }

    public long getTagCode() {
        return tagCode;
    }

    public int incrementAndGet() {
        return accessCount.incrementAndGet();
    }

    public int getAccessCount() {
        return accessCount.get();
    }
}
