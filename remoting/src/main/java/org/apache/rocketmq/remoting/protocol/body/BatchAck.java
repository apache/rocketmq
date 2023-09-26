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
package org.apache.rocketmq.remoting.protocol.body;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.rocketmq.remoting.protocol.BitSetSerializerDeserializer;

import java.io.Serializable;
import java.util.BitSet;

public class BatchAck implements Serializable {
    @JSONField(name = "c", alternateNames = {"consumerGroup"})
    private String consumerGroup;
    @JSONField(name = "t", alternateNames = {"topic"})
    private String topic;
    @JSONField(name = "r", alternateNames = {"retry"})
    private String retry; // "1" if is retry topic
    @JSONField(name = "so", alternateNames = {"startOffset"})
    private long startOffset;
    @JSONField(name = "q", alternateNames = {"queueId"})
    private int queueId;
    @JSONField(name = "rq", alternateNames = {"reviveQueueId"})
    private int reviveQueueId;
    @JSONField(name = "pt", alternateNames = {"popTime"})
    private long popTime;
    @JSONField(name = "it", alternateNames = {"invisibleTime"})
    private long invisibleTime;
    @JSONField(name = "b", alternateNames = {"bitSet"}, serializeUsing = BitSetSerializerDeserializer.class, deserializeUsing = BitSetSerializerDeserializer.class)
    private BitSet bitSet; // ack offsets bitSet

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getRetry() {
        return retry;
    }

    public void setRetry(String retry) {
        this.retry = retry;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getReviveQueueId() {
        return reviveQueueId;
    }

    public void setReviveQueueId(int reviveQueueId) {
        this.reviveQueueId = reviveQueueId;
    }

    public long getPopTime() {
        return popTime;
    }

    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public BitSet getBitSet() {
        return bitSet;
    }

    public void setBitSet(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    @Override
    public String toString() {
        return "BatchAck{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", topic='" + topic + '\'' +
                ", retry='" + retry + '\'' +
                ", startOffset=" + startOffset +
                ", queueId=" + queueId +
                ", reviveQueueId=" + reviveQueueId +
                ", popTime=" + popTime +
                ", invisibleTime=" + invisibleTime +
                ", bitSet=" + bitSet +
                '}';
    }
}
