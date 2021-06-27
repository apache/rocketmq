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
package org.apache.rocketmq.common.protocol.route;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * logical queue offset -> message queue offset mapping
 */
public class LogicalQueueRouteData implements Comparable<LogicalQueueRouteData> {
    private volatile int logicalQueueIndex = -1; /* -1 means not set */
    private volatile long logicalQueueDelta = -1; /* inclusive, -1 means not set, occurred in writeOnly state */

    private MessageQueue messageQueue;

    private volatile MessageQueueRouteState state = MessageQueueRouteState.Normal;

    private volatile long offsetDelta = 0; // valid when Normal/WriteOnly/ReadOnly
    private volatile long offsetMax = -1; // exclusive, valid when ReadOnly

    private volatile long firstMsgTimeMillis = -1; // valid when ReadOnly
    private volatile long lastMsgTimeMillis = -1; // valid when ReadOnly

    private String brokerAddr; /* not always set, only used by high availability forward */

    public LogicalQueueRouteData() {
    }

    public LogicalQueueRouteData(int logicalQueueIndex, long logicalQueueDelta,
        MessageQueue messageQueue, MessageQueueRouteState state, long offsetDelta, long offsetMax,
        long firstMsgTimeMillis,
        long lastMsgTimeMillis, String brokerAddr) {
        this.logicalQueueIndex = logicalQueueIndex;
        this.logicalQueueDelta = logicalQueueDelta;
        this.messageQueue = messageQueue;
        this.state = state;
        this.offsetDelta = offsetDelta;
        this.offsetMax = offsetMax;
        this.firstMsgTimeMillis = firstMsgTimeMillis;
        this.lastMsgTimeMillis = lastMsgTimeMillis;
        this.brokerAddr = brokerAddr;
    }

    public LogicalQueueRouteData(LogicalQueueRouteData queueRouteData) {
        copyFrom(queueRouteData);
    }

    public int getLogicalQueueIndex() {
        return logicalQueueIndex;
    }

    public void setLogicalQueueIndex(int logicalQueueIndex) {
        this.logicalQueueIndex = logicalQueueIndex;
    }

    public long getLogicalQueueDelta() {
        return logicalQueueDelta;
    }

    public void setLogicalQueueDelta(long logicalQueueDelta) {
        this.logicalQueueDelta = logicalQueueDelta;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public MessageQueueRouteState getState() {
        return state;
    }

    @JSONField(serialize = false)
    public int getStateOrdinal() {
        return state.ordinal();
    }

    public void setState(MessageQueueRouteState state) {
        this.state = state;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public long getOffsetDelta() {
        return offsetDelta;
    }

    public void setOffsetDelta(long offsetDelta) {
        this.offsetDelta = offsetDelta;
    }

    public long getOffsetMax() {
        return offsetMax;
    }

    public void setOffsetMax(long offsetMax) {
        this.offsetMax = offsetMax;
    }

    public long getFirstMsgTimeMillis() {
        return firstMsgTimeMillis;
    }

    public void setFirstMsgTimeMillis(long firstMsgTimeMillis) {
        this.firstMsgTimeMillis = firstMsgTimeMillis;
    }

    public long getLastMsgTimeMillis() {
        return lastMsgTimeMillis;
    }

    public void setLastMsgTimeMillis(long lastMsgTimeMillis) {
        this.lastMsgTimeMillis = lastMsgTimeMillis;
    }

    @Override public String toString() {
        return "LogicalQueueRouteData{" +
            "logicalQueueIndex=" + logicalQueueIndex +
            ", logicalQueueDelta=" + logicalQueueDelta +
            ", messageQueue=" + messageQueue +
            ", state=" + state +
            ", offsetDelta=" + offsetDelta +
            ", offsetMax=" + offsetMax +
            ", firstMsgTimeMillis=" + firstMsgTimeMillis +
            ", lastMsgTimeMillis=" + lastMsgTimeMillis +
            ", brokerAddr='" + brokerAddr + '\'' +
            '}';
    }

    public void copyFrom(LogicalQueueRouteData queueRouteData) {
        this.logicalQueueIndex = queueRouteData.logicalQueueIndex;
        this.logicalQueueDelta = queueRouteData.logicalQueueDelta;
        this.messageQueue = new MessageQueue(queueRouteData.getMessageQueue());
        this.state = queueRouteData.state;
        this.offsetDelta = queueRouteData.offsetDelta;
        this.offsetMax = queueRouteData.offsetMax;
        this.firstMsgTimeMillis = queueRouteData.firstMsgTimeMillis;
        this.lastMsgTimeMillis = queueRouteData.lastMsgTimeMillis;
        this.brokerAddr = queueRouteData.brokerAddr;
    }

    public long toLogicalQueueOffset(long messageQueueOffset) {
        return this.logicalQueueDelta < 0 ? -1 : messageQueueOffset - this.offsetDelta + this.logicalQueueDelta;
    }

    public long toMessageQueueOffset(long logicalQueueOffset) {
        return logicalQueueOffset - this.logicalQueueDelta + this.offsetDelta;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LogicalQueueRouteData that = (LogicalQueueRouteData) o;
        return logicalQueueIndex == that.logicalQueueIndex && logicalQueueDelta == that.logicalQueueDelta && offsetDelta == that.offsetDelta && offsetMax == that.offsetMax && firstMsgTimeMillis == that.firstMsgTimeMillis && lastMsgTimeMillis == that.lastMsgTimeMillis && Objects.equal(messageQueue, that.messageQueue) && state == that.state && Objects.equal(brokerAddr, that.brokerAddr);
    }

    @Override public int hashCode() {
        return Objects.hashCode(logicalQueueIndex, logicalQueueDelta, messageQueue, state, offsetDelta, offsetMax, firstMsgTimeMillis, lastMsgTimeMillis, brokerAddr);
    }

    @JSONField(serialize = false)
    public long getMessagesCount() {
        return this.offsetDelta >= 0 && this.offsetMax >= 0 ? this.offsetMax - this.offsetDelta : 0L;
    }

    @JSONField(serialize = false)
    public boolean isWritable() {
        return MessageQueueRouteState.Normal.equals(state) || MessageQueueRouteState.WriteOnly.equals(state);
    }

    @JSONField(serialize = false)
    public boolean isReadable() {
        return MessageQueueRouteState.Normal.equals(state) || MessageQueueRouteState.ReadOnly.equals(state);
    }

    @JSONField(serialize = false)
    public boolean isExpired() {
        return MessageQueueRouteState.Expired.equals(state);
    }

    @JSONField(serialize = false)
    public boolean isWriteOnly() {
        return MessageQueueRouteState.WriteOnly.equals(state);
    }

    @JSONField(serialize = false)
    public int getQueueId() {
        return messageQueue.getQueueId();
    }

    @JSONField(serialize = false)
    public String getBrokerName() {
        return messageQueue.getBrokerName();
    }

    @JSONField(serialize = false)
    public String getTopic() {
        return messageQueue.getTopic();
    }



    /**
     * First compare logicalQueueDelta, negative delta must be ordered in the last;
     * then compare state's ordinal;
     * then compare messageQueue, nulls first;
     * then compare offsetDelta.
     */
    @Override
    public int compareTo(LogicalQueueRouteData o) {
        long x = this.getLogicalQueueDelta();
        long y = o.getLogicalQueueDelta();
        int result;
        {
            if (x >= 0 && y >= 0) {
                result = MixAll.compareLong(x, y);
            } else if (x < 0 && y < 0) {
                result = MixAll.compareLong(-x, -y);
            } else if (x < 0) {
                // o1 < 0 && o2 >= 0
                result = 1;
            } else {
                // o1 >= 0 && o2 < 0
                result = -1;
            }
        }
        if (result == 0) {
            result = MixAll.compareInteger(this.state.ordinal(), o.state.ordinal());
        }
        if (result == 0) {
            if (this.messageQueue == null) {
                if (o.messageQueue != null) {
                    result = -1;
                }
            } else {
                if (o.messageQueue != null) {
                    result = this.messageQueue.compareTo(o.messageQueue);
                } else {
                    result = 1;
                }
            }
        }
        if (result == 0) {
            result = MixAll.compareLong(this.offsetDelta, o.offsetDelta);
        }
        return result;
    }

    public static final Predicate<LogicalQueueRouteData> READABLE_PREDICT = new Predicate<LogicalQueueRouteData>() {
        @Override public boolean apply(LogicalQueueRouteData input) {
            return input != null && input.isReadable();
        }
    };
}
