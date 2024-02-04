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

package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.store.ConsumeQueueExt;

import java.nio.ByteBuffer;

public class CqUnit {
    private final long queueOffset;
    private final int size;
    private final long pos;
    private final short batchNum;
    /**
     * Be careful, the tagsCode is reused as an address for extent file. To prevent accident mistake, we follow the
     * rules: 1. If the cqExtUnit is not null, make tagsCode == cqExtUnit.getTagsCode() 2. If the cqExtUnit is null, and
     * the tagsCode is smaller than 0, it is an invalid tagsCode, which means failed to get cqExtUnit by address
     */
    private long tagsCode;
    private ConsumeQueueExt.CqExtUnit cqExtUnit;
    private final ByteBuffer nativeBuffer;
    private final int compactedOffset;

    public CqUnit(long queueOffset, long pos, int size, long tagsCode) {
        this(queueOffset, pos, size, tagsCode, (short) 1, 0, null);
    }

    public CqUnit(long queueOffset, long pos, int size, long tagsCode, short batchNum, int compactedOffset, ByteBuffer buffer) {
        this.queueOffset = queueOffset;
        this.pos = pos;
        this.size = size;
        this.tagsCode = tagsCode;
        this.batchNum = batchNum;

        this.nativeBuffer = buffer;
        this.compactedOffset = compactedOffset;
    }

    public int getSize() {
        return size;
    }

    public long getPos() {
        return pos;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public Long getValidTagsCodeAsLong() {
        if (!isTagsCodeValid()) {
            return null;
        }
        return tagsCode;
    }

    public boolean isTagsCodeValid() {
        return !ConsumeQueueExt.isExtAddr(tagsCode);
    }

    public ConsumeQueueExt.CqExtUnit getCqExtUnit() {
        return cqExtUnit;
    }

    public void setCqExtUnit(ConsumeQueueExt.CqExtUnit cqExtUnit) {
        this.cqExtUnit = cqExtUnit;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public short getBatchNum() {
        return batchNum;
    }

    public void correctCompactOffset(int correctedOffset) {
        this.nativeBuffer.putInt(correctedOffset);
    }

    public int getCompactedOffset() {
        return compactedOffset;
    }

    @Override
    public String toString() {
        return "CqUnit{" +
                "queueOffset=" + queueOffset +
                ", size=" + size +
                ", pos=" + pos +
                ", batchNum=" + batchNum +
                ", compactedOffset=" + compactedOffset +
                '}';
    }
}
