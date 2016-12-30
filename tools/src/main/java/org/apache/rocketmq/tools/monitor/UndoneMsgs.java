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

package org.apache.rocketmq.tools.monitor;

public class UndoneMsgs {
    private String consumerGroup;
    private String topic;

    private long undoneMsgsTotal;

    private long undoneMsgsSingleMQ;

    private long undoneMsgsDelayTimeMills;

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

    public long getUndoneMsgsTotal() {
        return undoneMsgsTotal;
    }

    public void setUndoneMsgsTotal(long undoneMsgsTotal) {
        this.undoneMsgsTotal = undoneMsgsTotal;
    }

    public long getUndoneMsgsSingleMQ() {
        return undoneMsgsSingleMQ;
    }

    public void setUndoneMsgsSingleMQ(long undoneMsgsSingleMQ) {
        this.undoneMsgsSingleMQ = undoneMsgsSingleMQ;
    }

    public long getUndoneMsgsDelayTimeMills() {
        return undoneMsgsDelayTimeMills;
    }

    public void setUndoneMsgsDelayTimeMills(long undoneMsgsDelayTimeMills) {
        this.undoneMsgsDelayTimeMills = undoneMsgsDelayTimeMills;
    }

    @Override
    public String toString() {
        return "UndoneMsgs [consumerGroup=" + consumerGroup + ", topic=" + topic + ", undoneMsgsTotal="
            + undoneMsgsTotal + ", undoneMsgsSingleMQ=" + undoneMsgsSingleMQ
            + ", undoneMsgsDelayTimeMills=" + undoneMsgsDelayTimeMills + "]";
    }
}
