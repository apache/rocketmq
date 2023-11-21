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

package org.apache.rocketmq.tieredstore.index;

import java.nio.ByteBuffer;

public class IndexItem {

    public static final int INDEX_ITEM_SIZE = 32;
    public static final int COMPACT_INDEX_ITEM_SIZE = 28;

    private final int hashCode;
    private final int topicId;
    private final int queueId;
    private final long offset;
    private final int size;
    private final int timeDiff;
    private final int itemIndex;

    public IndexItem(int topicId, int queueId, long offset, int size, int hashCode, int timeDiff, int itemIndex) {
        this.hashCode = hashCode;
        this.topicId = topicId;
        this.queueId = queueId;
        this.offset = offset;
        this.size = size;
        this.timeDiff = timeDiff;
        this.itemIndex = itemIndex;
    }

    public IndexItem(byte[] bytes) {
        if (bytes == null ||
            bytes.length != INDEX_ITEM_SIZE &&
                bytes.length != COMPACT_INDEX_ITEM_SIZE) {
            throw new IllegalArgumentException("Byte array length not correct");
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        hashCode = byteBuffer.getInt(0);
        topicId = byteBuffer.getInt(4);
        queueId = byteBuffer.getInt(8);
        offset = byteBuffer.getLong(12);
        size = byteBuffer.getInt(20);
        timeDiff = byteBuffer.getInt(24);
        itemIndex = bytes.length == INDEX_ITEM_SIZE ? byteBuffer.getInt(28) : 0;
    }

    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(32);
        byteBuffer.putInt(0, hashCode);
        byteBuffer.putInt(4, topicId);
        byteBuffer.putInt(8, queueId);
        byteBuffer.putLong(12, offset);
        byteBuffer.putInt(20, size);
        byteBuffer.putInt(24, timeDiff);
        byteBuffer.putInt(28, itemIndex);
        return byteBuffer;
    }

    public int getHashCode() {
        return hashCode;
    }

    public int getTopicId() {
        return topicId;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public int getTimeDiff() {
        return timeDiff;
    }

    public int getItemIndex() {
        return itemIndex;
    }

    @Override
    public String toString() {
        return "IndexItem{" +
            "hashCode=" + hashCode +
            ", topicId=" + topicId +
            ", queueId=" + queueId +
            ", offset=" + offset +
            ", size=" + size +
            ", timeDiff=" + timeDiff +
            ", position=" + itemIndex +
            '}';
    }
}
