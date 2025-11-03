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
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.rpc.RpcRequestHeader;

public class PopLiteMessageRequestHeader extends RpcRequestHeader {

    @CFNotNull
    private String clientId;
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String topic;
    @CFNotNull
    private int maxMsgNum;
    @CFNotNull
    private long invisibleTime;
    @CFNotNull
    private long pollTime;
    @CFNotNull
    private long bornTime;

    private String attemptId;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

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

    public int getMaxMsgNum() {
        return maxMsgNum;
    }

    public void setMaxMsgNum(int maxMsgNum) {
        this.maxMsgNum = maxMsgNum;
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

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(long bornTime) {
        this.bornTime = bornTime;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(String attemptId) {
        this.attemptId = attemptId;
    }

    public boolean isTimeoutTooMuch() {
        return System.currentTimeMillis() - bornTime - pollTime > 500;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("consumerGroup", consumerGroup)
            .add("topic", topic)
            .add("maxMsgNum", maxMsgNum)
            .add("invisibleTime", invisibleTime)
            .add("pollTime", pollTime)
            .add("bornTime", bornTime)
            .add("attemptId", attemptId)
            .add("clientId", clientId)
            .toString();
    }
}
