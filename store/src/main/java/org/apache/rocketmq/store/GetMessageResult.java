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
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMappedList;
    private final List<ByteBuffer> messageBufferList;

    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;

    private int bufferTotalSize = 0;

    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;

    public GetMessageResult() {
        messageMappedList = new ArrayList<>(100);
        messageBufferList = new ArrayList<>(100);
    }

    public GetMessageResult(int resultSize) {
        messageMappedList = new ArrayList<>(resultSize);
        messageBufferList = new ArrayList<>(resultSize);
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

    public List<SelectMappedBufferResult> getMessageMappedList() {
        return messageMappedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mappedBuffer) {
        this.messageMappedList.add(mappedBuffer);
        this.messageBufferList.add(mappedBuffer.getByteBuffer());
        this.bufferTotalSize += mappedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mappedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMappedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public int getMessageCount() {
        return this.messageMappedList.size();
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

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
            + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }

}
