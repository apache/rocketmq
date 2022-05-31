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
package org.apache.rocketmq.common.protocol.header;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class PopMessageRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String consumerGroup;
    @CFNotNull
    private String topic;
    @CFNotNull
    private int queueId;
    @CFNotNull
    private int maxMsgNums;
    @CFNotNull
    private long invisibleTime;
    @CFNotNull
    private long pollTime;
    @CFNotNull
    private long bornTime;
    @CFNotNull
    private int initMode;

    private String expType;
    private String exp;

    /**
     * marked as order consume, if true
     * 1. not commit offset
     * 2. not pop retry, because no retry
     * 3. not append check point, because no retry
     */
    private Boolean order = Boolean.FALSE;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public void setInitMode(int initMode) {
        this.initMode = initMode;
    }

    public int getInitMode() {
        return initMode;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public long getPollTime() {
        return pollTime;
    }

    public void setPollTime(long pollTime) {
        this.pollTime = pollTime;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(long bornTime) {
        this.bornTime = bornTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        if (queueId < 0) {
            return -1;
        }
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }


    public int getMaxMsgNums() {
        return maxMsgNums;
    }

    public void setMaxMsgNums(int maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }

    public boolean isTimeoutTooMuch() {
        return System.currentTimeMillis() - bornTime - pollTime > 500;
    }

    public String getExpType() {
        return expType;
    }

    public void setExpType(String expType) {
        this.expType = expType;
    }

    public String getExp() {
        return exp;
    }

    public void setExp(String exp) {
        this.exp = exp;
    }

    public Boolean getOrder() {
        return order;
    }

    public void setOrder(Boolean order) {
        this.order = order;
    }

    public boolean isOrder() {
        return this.order != null && this.order.booleanValue();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("consumerGroup", consumerGroup)
            .add("topic", topic)
            .add("queueId", queueId)
            .add("maxMsgNums", maxMsgNums)
            .add("invisibleTime", invisibleTime)
            .add("pollTime", pollTime)
            .add("bornTime", bornTime)
            .add("initMode", initMode)
            .add("expType", expType)
            .add("exp", exp)
            .add("order", order)
            .toString();
    }
}
