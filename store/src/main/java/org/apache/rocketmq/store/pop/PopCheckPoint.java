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
import java.util.Objects;

public class PopCheckPoint implements Comparable<PopCheckPoint> {
    @JSONField(name = "so")
    private long startOffset;
    @JSONField(name = "pt")
    private long popTime;
    @JSONField(name = "it")
    private long invisibleTime;
    @JSONField(name = "bm")
    private int bitMap;
    @JSONField(name = "n")
    private byte num;
    @JSONField(name = "q")
    private int queueId;
    @JSONField(name = "t")
    private String topic;
    private String cid;
    @JSONField(name = "ro")
    private long reviveOffset;
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

    public int indexOfAck(long ackOffset) {
        if (ackOffset < startOffset) {
            return -1;
        }

        // old version of checkpoint
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {

            if (ackOffset - startOffset < num) {
                return (int) (ackOffset - startOffset);
            }

            return -1;
        }

        // new version of checkpoint
        return queueOffsetDiff.indexOf((int) (ackOffset - startOffset));
    }

    public long ackOffsetByIndex(byte index) {
        // old version of checkpoint
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

    /**
     * Orders checkpoints by start offset, then by the fields that identify a checkpoint.
     * <p>
     * The previous implementation was {@code (int) (this.startOffset - o.startOffset)}. startOffset is a
     * 64-bit queue offset, so the subtraction is 64-bit and casting the result to int discards the high
     * 32 bits; whenever two offsets differ by more than {@link Integer#MAX_VALUE} the sign of the result
     * can be inverted. That breaks the total-order contract (antisymmetry/transitivity) that sorted
     * structures rely on. The ordering is load-bearing: {@code inflightReviveRequestMap} in
     * PopReviveService is a {@code TreeMap} keyed by this type and uses containsKey/get/remove/firstKey
     * on it, so a non-transitive comparator makes it miss present keys, fail to evict entries and pick
     * the wrong entry to commit, which in turn advances the revive offset incorrectly. {@link Long#compare}
     * keeps the full 64-bit ordering.
     * <p>
     * Comparing only startOffset is also not enough to identify a checkpoint: PopReviveService processes
     * a single revive queue that carries checkpoints from many original topics and consumer groups whose
     * independent startOffsets can legitimately collide. The remaining fields are deterministic
     * tie-breakers that mirror the merge key used when the checkpoints are read back (topic, cid,
     * queueId, popTime, brokerName) plus reviveOffset, the unique position of the checkpoint in the
     * revive queue. They keep the ordering consistent with {@link #equals(Object)}/hashCode so two
     * distinct checkpoints never compare equal and neither can silently overwrite the other in a map.
     */
    @Override
    public int compareTo(PopCheckPoint o) {
        int cmp = Long.compare(this.getStartOffset(), o.getStartOffset());
        if (cmp != 0) {
            return cmp;
        }
        cmp = compareNullable(this.getTopic(), o.getTopic());
        if (cmp != 0) {
            return cmp;
        }
        cmp = compareNullable(this.getCId(), o.getCId());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.getQueueId(), o.getQueueId());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Long.compare(this.getPopTime(), o.getPopTime());
        if (cmp != 0) {
            return cmp;
        }
        cmp = compareNullable(this.getBrokerName(), o.getBrokerName());
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.getReviveOffset(), o.getReviveOffset());
    }

    /**
     * Null-safe String ordering. topic, cid and brokerName are nullable (a mock checkpoint created for a
     * late ack may carry a null brokerName), so a plain {@link String#compareTo} would throw for those
     * keys while the map only needs a stable ordering.
     */
    private static int compareNullable(String left, String right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        if (right == null) {
            return 1;
        }
        return left.compareTo(right);
    }

    /**
     * Value equality over the same identity fields used by {@link #compareTo(PopCheckPoint)}. Without it
     * the class fell back to identity equality, so {@code compareTo} == 0 and {@code equals} disagreed
     * for two checkpoints sharing a startOffset, and one of them could be dropped from a sorted map.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PopCheckPoint)) {
            return false;
        }
        // Keep this field list identical to the compareTo tie-breakers above: equals/hashCode and
        // compareTo must agree on identity, or a TreeMap keyed by this type drops distinct entries.
        PopCheckPoint other = (PopCheckPoint) o;
        return startOffset == other.startOffset
            && queueId == other.queueId
            && popTime == other.popTime
            && reviveOffset == other.reviveOffset
            && Objects.equals(topic, other.topic)
            && Objects.equals(cid, other.cid)
            && Objects.equals(brokerName, other.brokerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startOffset, popTime, queueId, topic, cid, reviveOffset, brokerName);
    }
}
