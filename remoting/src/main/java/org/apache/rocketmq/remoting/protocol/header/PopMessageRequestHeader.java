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
package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

@RocketMQAction(value = RequestCode.POP_MESSAGE, action = Action.SUB)
public class PopMessageRequestHeader extends TopicQueueRequestHeader {
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
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

    private String attemptId;

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

    public Integer getQueueId() {
        if (queueId < 0) {
            return -1;
        }
        return queueId;
    }

    @Override
    public void setQueueId(Integer queueId) {
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

    public String getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(String attemptId) {
        this.attemptId = attemptId;
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
            .add("attemptId", attemptId)
            .toString();
    }
}
