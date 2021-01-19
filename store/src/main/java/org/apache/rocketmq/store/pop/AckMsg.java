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

public class AckMsg {
    private long ackOffset;
    private long startOffset;
    private String consumerGroup;
    private String topic;
    private int queueId;
    private long popTime;

    public long getPt() {
        return popTime;
    }

    public void setPt(long popTime) {
        this.popTime = popTime;
    }

    public void setQ(int queueId) {
        this.queueId = queueId;
    }

    public int getQ() {
        return queueId;
    }

    public void setT(String topic) {
        this.topic = topic;
    }

    public String getT() {
        return topic;
    }

    public long getAo() {
        return ackOffset;
    }

    public String getC() {
        return consumerGroup;
    }

    public void setC(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void setAo(long ackOffset) {
        this.ackOffset = ackOffset;
    }

    public long getSo() {
        return startOffset;
    }

    public void setSo(long startOffset) {
        this.startOffset = startOffset;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AckMsg{");
        sb.append("ackOffset=").append(ackOffset);
        sb.append(", startOffset=").append(startOffset);
        sb.append(", consumerGroup='").append(consumerGroup).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append(", popTime=").append(popTime);
        sb.append('}');
        return sb.toString();
    }
}
