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
package io.openmessaging.rocketmq.config;

import io.openmessaging.PropertyKeys;
import io.openmessaging.rocketmq.domain.NonStandardKeys;

public class ClientConfig implements PropertyKeys, NonStandardKeys {
    private String omsDriverImpl;
    private String omsAccessPoints;
    private String omsNamespace;
    private String omsProducerId;
    private String omsConsumerId;
    private int omsOperationTimeout = 5000;
    private String omsRoutingName;
    private String omsOperatorName;
    private String omsDstQueue;
    private String omsSrcTopic;
    private String rmqConsumerGroup;
    private String rmqProducerGroup = "__OMS_PRODUCER_DEFAULT_GROUP";
    private int rmqMaxRedeliveryTimes = 16;
    private int rmqMessageConsumeTimeout = 15; //In minutes
    private int rmqMaxConsumeThreadNums = 64;
    private int rmqMinConsumeThreadNums = 20;
    private String rmqMessageDestination;
    private int rmqPullMessageBatchNums = 32;
    private int rmqPullMessageCacheCapacity = 1000;

    public String getOmsDriverImpl() {
        return omsDriverImpl;
    }

    public void setOmsDriverImpl(final String omsDriverImpl) {
        this.omsDriverImpl = omsDriverImpl;
    }

    public String getOmsAccessPoints() {
        return omsAccessPoints;
    }

    public void setOmsAccessPoints(final String omsAccessPoints) {
        this.omsAccessPoints = omsAccessPoints;
    }

    public String getOmsNamespace() {
        return omsNamespace;
    }

    public void setOmsNamespace(final String omsNamespace) {
        this.omsNamespace = omsNamespace;
    }

    public String getOmsProducerId() {
        return omsProducerId;
    }

    public void setOmsProducerId(final String omsProducerId) {
        this.omsProducerId = omsProducerId;
    }

    public String getOmsConsumerId() {
        return omsConsumerId;
    }

    public void setOmsConsumerId(final String omsConsumerId) {
        this.omsConsumerId = omsConsumerId;
    }

    public int getOmsOperationTimeout() {
        return omsOperationTimeout;
    }

    public void setOmsOperationTimeout(final int omsOperationTimeout) {
        this.omsOperationTimeout = omsOperationTimeout;
    }

    public String getOmsRoutingName() {
        return omsRoutingName;
    }

    public void setOmsRoutingName(final String omsRoutingName) {
        this.omsRoutingName = omsRoutingName;
    }

    public String getOmsOperatorName() {
        return omsOperatorName;
    }

    public void setOmsOperatorName(final String omsOperatorName) {
        this.omsOperatorName = omsOperatorName;
    }

    public String getOmsDstQueue() {
        return omsDstQueue;
    }

    public void setOmsDstQueue(final String omsDstQueue) {
        this.omsDstQueue = omsDstQueue;
    }

    public String getOmsSrcTopic() {
        return omsSrcTopic;
    }

    public void setOmsSrcTopic(final String omsSrcTopic) {
        this.omsSrcTopic = omsSrcTopic;
    }

    public String getRmqConsumerGroup() {
        return rmqConsumerGroup;
    }

    public void setRmqConsumerGroup(final String rmqConsumerGroup) {
        this.rmqConsumerGroup = rmqConsumerGroup;
    }

    public String getRmqProducerGroup() {
        return rmqProducerGroup;
    }

    public void setRmqProducerGroup(final String rmqProducerGroup) {
        this.rmqProducerGroup = rmqProducerGroup;
    }

    public int getRmqMaxRedeliveryTimes() {
        return rmqMaxRedeliveryTimes;
    }

    public void setRmqMaxRedeliveryTimes(final int rmqMaxRedeliveryTimes) {
        this.rmqMaxRedeliveryTimes = rmqMaxRedeliveryTimes;
    }

    public int getRmqMessageConsumeTimeout() {
        return rmqMessageConsumeTimeout;
    }

    public void setRmqMessageConsumeTimeout(final int rmqMessageConsumeTimeout) {
        this.rmqMessageConsumeTimeout = rmqMessageConsumeTimeout;
    }

    public int getRmqMaxConsumeThreadNums() {
        return rmqMaxConsumeThreadNums;
    }

    public void setRmqMaxConsumeThreadNums(final int rmqMaxConsumeThreadNums) {
        this.rmqMaxConsumeThreadNums = rmqMaxConsumeThreadNums;
    }

    public int getRmqMinConsumeThreadNums() {
        return rmqMinConsumeThreadNums;
    }

    public void setRmqMinConsumeThreadNums(final int rmqMinConsumeThreadNums) {
        this.rmqMinConsumeThreadNums = rmqMinConsumeThreadNums;
    }

    public String getRmqMessageDestination() {
        return rmqMessageDestination;
    }

    public void setRmqMessageDestination(final String rmqMessageDestination) {
        this.rmqMessageDestination = rmqMessageDestination;
    }

    public int getRmqPullMessageBatchNums() {
        return rmqPullMessageBatchNums;
    }

    public void setRmqPullMessageBatchNums(final int rmqPullMessageBatchNums) {
        this.rmqPullMessageBatchNums = rmqPullMessageBatchNums;
    }

    public int getRmqPullMessageCacheCapacity() {
        return rmqPullMessageCacheCapacity;
    }

    public void setRmqPullMessageCacheCapacity(final int rmqPullMessageCacheCapacity) {
        this.rmqPullMessageCacheCapacity = rmqPullMessageCacheCapacity;
    }
}
