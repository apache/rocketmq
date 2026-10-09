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
package org.apache.rocketmq.store.pop;

import com.alibaba.fastjson2.annotation.JSONField;

import java.util.ArrayList;
import java.util.List;

/**
 * state check info for multi-messages pop from consume queue
 */
public class PopCheckPoint implements Comparable<PopCheckPoint> {
    @JSONField(name = "so")
    private long startOffset;
    /**
     * pop time, which is the time when message is popped
     * reviveTime = popTime + invisibleTime
     */
    @JSONField(name = "pt")
    private long popTime;
    /**
     * the invisible time of messages
     * default is 60s, it can be changed by MQ client
     */
    @JSONField(name = "it")
    private long invisibleTime;
    /**
     * store ack states of messages
     * one byte for each message
     */
    @JSONField(name = "bm")
    private int bitMap;
    /**
     * total number of messages
     */
    @JSONField(name = "n")
    private byte num;
    @JSONField(name = "q")
    private int queueId;
    @JSONField(name = "t")
    private String topic;
    /**
     * consumer group
     */
    private String cid;
    /**
     * revive offset, which is the consume queue offset of messageExt
     */
    @JSONField(name = "ro")
    private long reviveOffset;
    /**
     * Per-message offset differences from {@link #startOffset}.
     * queueOffsetDiff will not be null or empty in 5.*
     *
     * <p>When a batch of messages is popped, the queue offsets of the messages may not
     * be contiguous (e.g. batch messages, ConsumeQueue compaction, filter mismatch gaps).
     * This list records {@code actualQueueOffset - startOffset} for each message in the
     * batch, so that the system can correctly map an ack offset back to its index within
     * the checkpoint via {@link #indexOfAck}, and reconstruct the original offset via
     * {@link #ackOffsetByIndex}.
     *
     * <p>When this field is null or empty (old-version CK), offsets are assumed to be
     * {@code startOffset + index}.
     */
    @JSONField(name = "d")
    private List<Integer> queueOffsetDiff;
    @JSONField(name = "bn")
    String brokerName;
    @JSONField(name = "rp")
    String rePutTimes; // ck rePut times
    @JSONField(name = "sp")
    private boolean suspend; // nack without inc reconsume times, false default.

    public long getReviveOffset() {
        return reviveOffset;
    }

    public void setReviveOffset(long reviveOffset) {
        this.reviveOffset = reviveOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public long getPopTime() {
        return popTime;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public long getReviveTime() {
        return popTime + invisibleTime;
    }

    public int getBitMap() {
        return bitMap;
    }

    public void setBitMap(int bitMap) {
        this.bitMap = bitMap;
    }

    public byte getNum() {
        return num;
    }

    public void setNum(byte num) {
        this.num = num;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @JSONField(name = "c")
    public String getCId() {
        return cid;
    }

    @JSONField(name = "c")
    public void setCId(String cid) {
        this.cid = cid;
    }

    public List<Integer> getQueueOffsetDiff() {
        return queueOffsetDiff;
    }

    public void setQueueOffsetDiff(List<Integer> queueOffsetDiff) {
        this.queueOffsetDiff = queueOffsetDiff;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getRePutTimes() {
        return rePutTimes;
    }

    public void setRePutTimes(String rePutTimes) {
        this.rePutTimes = rePutTimes;
    }

    public boolean isSuspend() {
        return suspend;
    }

    public void setSuspend(boolean suspend) {
        this.suspend = suspend;
    }

    public void addDiff(int diff) {
        if (this.queueOffsetDiff == null) {
            this.queueOffsetDiff = new ArrayList<>(8);
        }
        this.queueOffsetDiff.add(diff);
    }

    /**
     * Map an ack offset to its index within the checkpoint batch.
     *
     * <p>The index is used to look up the corresponding bit in the {@link #bitMap}
     * (or in {@code PopCheckPointWrapper.bits}) and to retrieve the original
     * queue offset via {@link #ackOffsetByIndex}.
     *
     * @param ackOffset the queue offset being acked
     * @return the sub-message index (0-based), or -1 if the offset is not found
     *         in this checkpoint
     */
    public int indexOfAck(long ackOffset) {
        if (ackOffset < startOffset) {
            return -1;
        }

        // old version of checkpoint, this will not happen in 5.*
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {

            if (ackOffset - startOffset < num) {
                return (int) (ackOffset - startOffset);
            }

            return -1;
        }

        // new version of checkpoint
        return queueOffsetDiff.indexOf((int) (ackOffset - startOffset));
    }

    /**
     * get original queue offset by index.
     * the method name is miss-leading, it should be getQueueOffsetByIndex.
     * queueOffset  = startOffset + queueOffsetDiff[index]
     *
     * @param index sub-message index within this checkpoint (0-based)
     * @return the original queue offset in the consume queue
     */
    public long ackOffsetByIndex(byte index) {
        // old version of checkpoint, this will not happen in 5.*
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {
            return startOffset + index;
        }

        return startOffset + queueOffsetDiff.get(index);
    }

    public int parseRePutTimes() {
        if (null == rePutTimes) {
            return 0;
        }
        try {
            return Integer.parseInt(rePutTimes);
        } catch (Exception e) {
        }
        return Byte.MAX_VALUE;
    }

    @Override
    public String toString() {
        return "PopCheckPoint [topic=" + topic + ", cid=" + cid + ", queueId=" + queueId + ", startOffset=" + startOffset + ", bitMap=" + bitMap + ", num=" + num + ", reviveTime=" + getReviveTime()
            + ", reviveOffset=" + reviveOffset + ", diff=" + queueOffsetDiff + ", brokerName=" + brokerName + ", rePutTimes=" + rePutTimes + ", suspend=" + suspend + "]";
    }

    @Override
    public int compareTo(PopCheckPoint o) {
        return (int) (this.getStartOffset() - o.getStartOffset());
    }
}
