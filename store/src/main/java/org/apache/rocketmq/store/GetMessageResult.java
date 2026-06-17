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

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList;
    private final List<ByteBuffer> messageBufferList;
    private long[] messageQueueOffset;
    private int queueOffsetSize;

    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;

    private int bufferTotalSize = 0;

    private int messageCount = 0;

    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;
    private int commercialSizePerMsg = 4 * 1024;

    private long coldDataSum = 0L;

    private int filterMessageCount;

    public static final GetMessageResult NO_MATCH_LOGIC_QUEUE =
        new GetMessageResult(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, 0, 0, 0, Collections.emptyList(),
            Collections.emptyList(), new long[0], 0);

    public GetMessageResult() {
        messageMapedList = new ArrayList<>(100);
        messageBufferList = new ArrayList<>(100);
        messageQueueOffset = new long[100];
        queueOffsetSize = 0;
    }

    public GetMessageResult(int resultSize) {
        int initialCapacity = Math.min(Math.max(resultSize, 1), 256);
        messageMapedList = new ArrayList<>(initialCapacity);
        messageBufferList = new ArrayList<>(initialCapacity);
        messageQueueOffset = new long[initialCapacity];
        queueOffsetSize = 0;
    }

    private GetMessageResult(GetMessageStatus status, long nextBeginOffset, long minOffset, long maxOffset,
        List<SelectMappedBufferResult> messageMapedList, List<ByteBuffer> messageBufferList, long[] messageQueueOffset, int queueOffsetSize) {
        this.status = status;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messageMapedList = messageMapedList;
        this.messageBufferList = messageBufferList;
        this.messageQueueOffset = messageQueueOffset;
        this.queueOffsetSize = queueOffsetSize;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
        if (queueOffsetSize == messageQueueOffset.length) {
            long[] newArr = new long[queueOffsetSize + (queueOffsetSize >> 1)];
            System.arraycopy(messageQueueOffset, 0, newArr, 0, queueOffsetSize);
            messageQueueOffset = newArr;
        }
        messageQueueOffset[queueOffsetSize++] = queueOffset;
    }


    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset, final int batchNum) {
        addMessage(mapedBuffer, queueOffset);
        messageCount += batchNum - 1;
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    public void addQueueOffset(long offset) {
        if (queueOffsetSize == messageQueueOffset.length) {
            long[] newArr = new long[queueOffsetSize + (queueOffsetSize >> 1) + 1];
            System.arraycopy(messageQueueOffset, 0, newArr, 0, queueOffsetSize);
            messageQueueOffset = newArr;
        }
        messageQueueOffset[queueOffsetSize++] = offset;
    }

    public List<Long> getMessageQueueOffset() {
        final long[] arr = this.messageQueueOffset;
        final int size = this.queueOffsetSize;
        return new AbstractList<Long>() {
            @Override public Long get(int index) {
                if (index < 0 || index >= size) throw new IndexOutOfBoundsException();
                return arr[index];
            }
            @Override public int size() { return size; }
        };
    }

    public long getColdDataSum() {
        return coldDataSum;
    }

    public void setColdDataSum(long coldDataSum) {
        this.coldDataSum = coldDataSum;
    }

    public int getFilterMessageCount() {
        return filterMessageCount;
    }

    public void setFilterMessageCount(int filterMessageCount) {
        this.filterMessageCount = filterMessageCount;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize + ", messageCount=" + messageCount
            + ", filterMessageCount=" + filterMessageCount + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }
}
