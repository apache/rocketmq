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
package org.apache.rocketmq.broker.mqtrace;

import java.util.Map;

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ConsumeMessageContext {
    private String consumerGroup;
    private String topic;
    private Integer queueId;
    private String clientHost;
    private String storeHost;
    private Map<String, Long> messageIds;
    private int bodyLength;
    private boolean success;
    private String status;
    private Object mqTraceContext;
    private TopicConfig topicConfig;

    private String accountAuthType;
    private String accountOwnerParent;
    private String accountOwnerSelf;
    private int rcvMsgNum;
    private int rcvMsgSize;
    private BrokerStatsManager.StatsType rcvStat;
    private int commercialRcvMsgNum;

    private String commercialOwner;
    private BrokerStatsManager.StatsType commercialRcvStats;
    private int commercialRcvTimes;
    private int commercialRcvSize;
    private int filterMessageCount;

    private String namespace;
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

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public String getClientHost() {
        return clientHost;
    }

    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
    }

    public String getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(String storeHost) {
        this.storeHost = storeHost;
    }

    public Map<String, Long> getMessageIds() {
        return messageIds;
    }

    public void setMessageIds(Map<String, Long> messageIds) {
        this.messageIds = messageIds;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getMqTraceContext() {
        return mqTraceContext;
    }

    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }

    public TopicConfig getTopicConfig() {
        return topicConfig;
    }

    public void setTopicConfig(TopicConfig topicConfig) {
        this.topicConfig = topicConfig;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public String getAccountAuthType() {
        return accountAuthType;
    }

    public void setAccountAuthType(String accountAuthType) {
        this.accountAuthType = accountAuthType;
    }

    public String getAccountOwnerParent() {
        return accountOwnerParent;
    }

    public void setAccountOwnerParent(String accountOwnerParent) {
        this.accountOwnerParent = accountOwnerParent;
    }

    public String getAccountOwnerSelf() {
        return accountOwnerSelf;
    }

    public void setAccountOwnerSelf(String accountOwnerSelf) {
        this.accountOwnerSelf = accountOwnerSelf;
    }

    public int getRcvMsgNum() {
        return rcvMsgNum;
    }

    public void setRcvMsgNum(int rcvMsgNum) {
        this.rcvMsgNum = rcvMsgNum;
    }

    public int getRcvMsgSize() {
        return rcvMsgSize;
    }

    public void setRcvMsgSize(int rcvMsgSize) {
        this.rcvMsgSize = rcvMsgSize;
    }

    public BrokerStatsManager.StatsType getRcvStat() {
        return rcvStat;
    }

    public void setRcvStat(BrokerStatsManager.StatsType rcvStat) {
        this.rcvStat = rcvStat;
    }

    public int getCommercialRcvMsgNum() {
        return commercialRcvMsgNum;
    }

    public void setCommercialRcvMsgNum(int commercialRcvMsgNum) {
        this.commercialRcvMsgNum = commercialRcvMsgNum;
    }

    public String getCommercialOwner() {
        return commercialOwner;
    }

    public void setCommercialOwner(final String commercialOwner) {
        this.commercialOwner = commercialOwner;
    }

    public BrokerStatsManager.StatsType getCommercialRcvStats() {
        return commercialRcvStats;
    }

    public void setCommercialRcvStats(final BrokerStatsManager.StatsType commercialRcvStats) {
        this.commercialRcvStats = commercialRcvStats;
    }

    public int getCommercialRcvTimes() {
        return commercialRcvTimes;
    }

    public void setCommercialRcvTimes(final int commercialRcvTimes) {
        this.commercialRcvTimes = commercialRcvTimes;
    }

    public int getCommercialRcvSize() {
        return commercialRcvSize;
    }

    public void setCommercialRcvSize(final int commercialRcvSize) {
        this.commercialRcvSize = commercialRcvSize;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public int getFilterMessageCount() {
        return filterMessageCount;
    }

    public void setFilterMessageCount(int filterMessageCount) {
        this.filterMessageCount = filterMessageCount;
    }
}
