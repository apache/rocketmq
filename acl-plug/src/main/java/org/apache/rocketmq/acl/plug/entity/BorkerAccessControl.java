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
package org.apache.rocketmq.acl.plug.entity;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.acl.plug.annotation.RequestCode;


public class BorkerAccessControl extends AccessControl {

    public BorkerAccessControl() {

    }

    private Set<String> permitSendTopic = new HashSet<>();

    private Set<String> noPermitSendTopic = new HashSet<>();

    private Set<String> permitPullTopic = new HashSet<>();

    private Set<String> noPermitPullTopic = new HashSet<>();

    @RequestCode(code = 10)
    private boolean sendMessage = true;

    @RequestCode(code = 310)
    private boolean sendMessageV2 = true;

    @RequestCode(code = 320)
    private boolean sendBatchMessage = true;

    @RequestCode(code = 36)
    private boolean consumerSendMsgBack = true;

    @RequestCode(code = 11)
    private boolean pullMessage = true;

    @RequestCode(code = 12)
    private boolean queryMessage = true;

    @RequestCode(code = 33)
    private boolean viewMessageById = true;

    @RequestCode(code = 34)
    private boolean heartBeat = true;

    @RequestCode(code = 35)
    private boolean unregisterClient = true;

    @RequestCode(code = 46)
    private boolean checkClientConfig = true;

    @RequestCode(code = 38)
    private boolean getConsumerListByGroup = true;

    @RequestCode(code = 15)
    private boolean updateConsumerOffset = true;

    @RequestCode(code = 14)
    private boolean queryConsumerOffset = true;

    @RequestCode(code = 37)
    private boolean endTransaction = true;

    @RequestCode(code = 17)
    private boolean updateAndCreateTopic = true;

    @RequestCode(code = 215)
    private boolean deleteTopicInbroker = true;

    @RequestCode(code = 21)
    private boolean getAllTopicConfig = true;

    @RequestCode(code = 25)
    private boolean updateBrokerConfig = true;

    @RequestCode(code = 26)
    private boolean getBrokerConfig = true;

    @RequestCode(code = 29)
    private boolean searchOffsetByTimestamp = true;

    @RequestCode(code = 30)
    private boolean getMaxOffset = true;

    @RequestCode(code = 31)
    private boolean getMixOffset = true;

    @RequestCode(code = 32)
    private boolean getEarliestMsgStoretime = true;

    @RequestCode(code = 28)
    private boolean getBrokerRuntimeInfo = true;

    @RequestCode(code = 41)
    private boolean lockBatchMQ = true;

    @RequestCode(code = 42)
    private boolean unlockBatchMQ = true;

    @RequestCode(code = 200)
    private boolean updateAndCreteSubscriptiongroup = true;

    @RequestCode(code = 201)
    private boolean getAllSubscriptiongroupConfig = true;

    @RequestCode(code = 207)
    private boolean deleteSubscriptiongroup = true;

    @RequestCode(code = 202)
    private boolean getTopicStatsInfo = true;

    @RequestCode(code = 203)
    private boolean getConsumerConnectionList = true;

    @RequestCode(code = 204)
    private boolean getProducerConnectionList = true;

    @RequestCode(code = 208)
    private boolean getConsumeStats = true;

    @RequestCode(code = 43)
    private boolean getAllConsumerOffset = true;

    @RequestCode(code = 25)
    private boolean getAllDelayOffset = true;

    @RequestCode(code = 222)
    private boolean invokeBrokerToresetOffset = true;

    @RequestCode(code = 300)
    private boolean queryTopicConsumByWho = true;

    @RequestCode(code = 301)
    private boolean registerFilterServer = true;

    @RequestCode(code = 303)
    private boolean queryConsumeTimeSpan = true;

    @RequestCode(code = 305)
    private boolean getSystemTopicListFromBroker = true;

    @RequestCode(code = 306)
    private boolean cleanExpiredConsumequeue = true;

    @RequestCode(code = 316)
    private boolean cleanUnusedTopic = true;

    @RequestCode(code = 307)
    private boolean getConsumerRunningInfo = true;

    @RequestCode(code = 308)
    private boolean queryCorrectionOffset = true;

    @RequestCode(code = 309)
    private boolean consumeMessageDirectly = true;

    @RequestCode(code = 314)
    private boolean cloneGroupOffset = true;

    @RequestCode(code = 315)
    private boolean viewBrokerStatsData = true;

    @RequestCode(code = 317)
    private boolean getBrokerConsumeStats = true;

    @RequestCode(code = 321)
    private boolean queryConsumeQueue = true;

    public Set<String> getPermitSendTopic() {
        return permitSendTopic;
    }

    public void setPermitSendTopic(Set<String> permitSendTopic) {
        this.permitSendTopic = permitSendTopic;
    }

    public Set<String> getNoPermitSendTopic() {
        return noPermitSendTopic;
    }

    public void setNoPermitSendTopic(Set<String> noPermitSendTopic) {
        this.noPermitSendTopic = noPermitSendTopic;
    }

    public Set<String> getPermitPullTopic() {
        return permitPullTopic;
    }

    public void setPermitPullTopic(Set<String> permitPullTopic) {
        this.permitPullTopic = permitPullTopic;
    }

    public Set<String> getNoPermitPullTopic() {
        return noPermitPullTopic;
    }

    public void setNoPermitPullTopic(Set<String> noPermitPullTopic) {
        this.noPermitPullTopic = noPermitPullTopic;
    }

    public boolean isSendMessage() {
        return sendMessage;
    }

    public void setSendMessage(boolean sendMessage) {
        this.sendMessage = sendMessage;
    }

    public boolean isSendMessageV2() {
        return sendMessageV2;
    }

    public void setSendMessageV2(boolean sendMessageV2) {
        this.sendMessageV2 = sendMessageV2;
    }

    public boolean isSendBatchMessage() {
        return sendBatchMessage;
    }

    public void setSendBatchMessage(boolean sendBatchMessage) {
        this.sendBatchMessage = sendBatchMessage;
    }

    public boolean isConsumerSendMsgBack() {
        return consumerSendMsgBack;
    }

    public void setConsumerSendMsgBack(boolean consumerSendMsgBack) {
        this.consumerSendMsgBack = consumerSendMsgBack;
    }

    public boolean isPullMessage() {
        return pullMessage;
    }

    public void setPullMessage(boolean pullMessage) {
        this.pullMessage = pullMessage;
    }

    public boolean isQueryMessage() {
        return queryMessage;
    }

    public void setQueryMessage(boolean queryMessage) {
        this.queryMessage = queryMessage;
    }

    public boolean isViewMessageById() {
        return viewMessageById;
    }

    public void setViewMessageById(boolean viewMessageById) {
        this.viewMessageById = viewMessageById;
    }

    public boolean isHeartBeat() {
        return heartBeat;
    }

    public void setHeartBeat(boolean heartBeat) {
        this.heartBeat = heartBeat;
    }

    public boolean isUnregisterClient() {
        return unregisterClient;
    }

    public void setUnregisterClient(boolean unregisterClient) {
        this.unregisterClient = unregisterClient;
    }

    public boolean isCheckClientConfig() {
        return checkClientConfig;
    }

    public void setCheckClientConfig(boolean checkClientConfig) {
        this.checkClientConfig = checkClientConfig;
    }

    public boolean isGetConsumerListByGroup() {
        return getConsumerListByGroup;
    }

    public void setGetConsumerListByGroup(boolean getConsumerListByGroup) {
        this.getConsumerListByGroup = getConsumerListByGroup;
    }

    public boolean isUpdateConsumerOffset() {
        return updateConsumerOffset;
    }

    public void setUpdateConsumerOffset(boolean updateConsumerOffset) {
        this.updateConsumerOffset = updateConsumerOffset;
    }

    public boolean isQueryConsumerOffset() {
        return queryConsumerOffset;
    }

    public void setQueryConsumerOffset(boolean queryConsumerOffset) {
        this.queryConsumerOffset = queryConsumerOffset;
    }

    public boolean isEndTransaction() {
        return endTransaction;
    }

    public void setEndTransaction(boolean endTransaction) {
        this.endTransaction = endTransaction;
    }

    public boolean isUpdateAndCreateTopic() {
        return updateAndCreateTopic;
    }

    public void setUpdateAndCreateTopic(boolean updateAndCreateTopic) {
        this.updateAndCreateTopic = updateAndCreateTopic;
    }

    public boolean isDeleteTopicInbroker() {
        return deleteTopicInbroker;
    }

    public void setDeleteTopicInbroker(boolean deleteTopicInbroker) {
        this.deleteTopicInbroker = deleteTopicInbroker;
    }

    public boolean isGetAllTopicConfig() {
        return getAllTopicConfig;
    }

    public void setGetAllTopicConfig(boolean getAllTopicConfig) {
        this.getAllTopicConfig = getAllTopicConfig;
    }

    public boolean isUpdateBrokerConfig() {
        return updateBrokerConfig;
    }

    public void setUpdateBrokerConfig(boolean updateBrokerConfig) {
        this.updateBrokerConfig = updateBrokerConfig;
    }

    public boolean isGetBrokerConfig() {
        return getBrokerConfig;
    }

    public void setGetBrokerConfig(boolean getBrokerConfig) {
        this.getBrokerConfig = getBrokerConfig;
    }

    public boolean isSearchOffsetByTimestamp() {
        return searchOffsetByTimestamp;
    }

    public void setSearchOffsetByTimestamp(boolean searchOffsetByTimestamp) {
        this.searchOffsetByTimestamp = searchOffsetByTimestamp;
    }

    public boolean isGetMaxOffset() {
        return getMaxOffset;
    }

    public void setGetMaxOffset(boolean getMaxOffset) {
        this.getMaxOffset = getMaxOffset;
    }

    public boolean isGetMixOffset() {
        return getMixOffset;
    }

    public void setGetMixOffset(boolean getMixOffset) {
        this.getMixOffset = getMixOffset;
    }

    public boolean isGetEarliestMsgStoretime() {
        return getEarliestMsgStoretime;
    }

    public void setGetEarliestMsgStoretime(boolean getEarliestMsgStoretime) {
        this.getEarliestMsgStoretime = getEarliestMsgStoretime;
    }

    public boolean isGetBrokerRuntimeInfo() {
        return getBrokerRuntimeInfo;
    }

    public void setGetBrokerRuntimeInfo(boolean getBrokerRuntimeInfo) {
        this.getBrokerRuntimeInfo = getBrokerRuntimeInfo;
    }

    public boolean isLockBatchMQ() {
        return lockBatchMQ;
    }

    public void setLockBatchMQ(boolean lockBatchMQ) {
        this.lockBatchMQ = lockBatchMQ;
    }

    public boolean isUnlockBatchMQ() {
        return unlockBatchMQ;
    }

    public void setUnlockBatchMQ(boolean unlockBatchMQ) {
        this.unlockBatchMQ = unlockBatchMQ;
    }

    public boolean isUpdateAndCreteSubscriptiongroup() {
        return updateAndCreteSubscriptiongroup;
    }

    public void setUpdateAndCreteSubscriptiongroup(boolean updateAndCreteSubscriptiongroup) {
        this.updateAndCreteSubscriptiongroup = updateAndCreteSubscriptiongroup;
    }

    public boolean isGetAllSubscriptiongroupConfig() {
        return getAllSubscriptiongroupConfig;
    }

    public void setGetAllSubscriptiongroupConfig(boolean getAllSubscriptiongroupConfig) {
        this.getAllSubscriptiongroupConfig = getAllSubscriptiongroupConfig;
    }

    public boolean isDeleteSubscriptiongroup() {
        return deleteSubscriptiongroup;
    }

    public void setDeleteSubscriptiongroup(boolean deleteSubscriptiongroup) {
        this.deleteSubscriptiongroup = deleteSubscriptiongroup;
    }

    public boolean isGetTopicStatsInfo() {
        return getTopicStatsInfo;
    }

    public void setGetTopicStatsInfo(boolean getTopicStatsInfo) {
        this.getTopicStatsInfo = getTopicStatsInfo;
    }

    public boolean isGetConsumerConnectionList() {
        return getConsumerConnectionList;
    }

    public void setGetConsumerConnectionList(boolean getConsumerConnectionList) {
        this.getConsumerConnectionList = getConsumerConnectionList;
    }

    public boolean isGetProducerConnectionList() {
        return getProducerConnectionList;
    }

    public void setGetProducerConnectionList(boolean getProducerConnectionList) {
        this.getProducerConnectionList = getProducerConnectionList;
    }

    public boolean isGetConsumeStats() {
        return getConsumeStats;
    }

    public void setGetConsumeStats(boolean getConsumeStats) {
        this.getConsumeStats = getConsumeStats;
    }

    public boolean isGetAllConsumerOffset() {
        return getAllConsumerOffset;
    }

    public void setGetAllConsumerOffset(boolean getAllConsumerOffset) {
        this.getAllConsumerOffset = getAllConsumerOffset;
    }

    public boolean isGetAllDelayOffset() {
        return getAllDelayOffset;
    }

    public void setGetAllDelayOffset(boolean getAllDelayOffset) {
        this.getAllDelayOffset = getAllDelayOffset;
    }

    public boolean isInvokeBrokerToresetOffset() {
        return invokeBrokerToresetOffset;
    }

    public void setInvokeBrokerToresetOffset(boolean invokeBrokerToresetOffset) {
        this.invokeBrokerToresetOffset = invokeBrokerToresetOffset;
    }

    public boolean isQueryTopicConsumByWho() {
        return queryTopicConsumByWho;
    }

    public void setQueryTopicConsumByWho(boolean queryTopicConsumByWho) {
        this.queryTopicConsumByWho = queryTopicConsumByWho;
    }

    public boolean isRegisterFilterServer() {
        return registerFilterServer;
    }

    public void setRegisterFilterServer(boolean registerFilterServer) {
        this.registerFilterServer = registerFilterServer;
    }

    public boolean isQueryConsumeTimeSpan() {
        return queryConsumeTimeSpan;
    }

    public void setQueryConsumeTimeSpan(boolean queryConsumeTimeSpan) {
        this.queryConsumeTimeSpan = queryConsumeTimeSpan;
    }

    public boolean isGetSystemTopicListFromBroker() {
        return getSystemTopicListFromBroker;
    }

    public void setGetSystemTopicListFromBroker(boolean getSystemTopicListFromBroker) {
        this.getSystemTopicListFromBroker = getSystemTopicListFromBroker;
    }

    public boolean isCleanExpiredConsumequeue() {
        return cleanExpiredConsumequeue;
    }

    public void setCleanExpiredConsumequeue(boolean cleanExpiredConsumequeue) {
        this.cleanExpiredConsumequeue = cleanExpiredConsumequeue;
    }

    public boolean isCleanUnusedTopic() {
        return cleanUnusedTopic;
    }

    public void setCleanUnusedTopic(boolean cleanUnusedTopic) {
        this.cleanUnusedTopic = cleanUnusedTopic;
    }

    public boolean isGetConsumerRunningInfo() {
        return getConsumerRunningInfo;
    }

    public void setGetConsumerRunningInfo(boolean getConsumerRunningInfo) {
        this.getConsumerRunningInfo = getConsumerRunningInfo;
    }

    public boolean isQueryCorrectionOffset() {
        return queryCorrectionOffset;
    }

    public void setQueryCorrectionOffset(boolean queryCorrectionOffset) {
        this.queryCorrectionOffset = queryCorrectionOffset;
    }

    public boolean isConsumeMessageDirectly() {
        return consumeMessageDirectly;
    }

    public void setConsumeMessageDirectly(boolean consumeMessageDirectly) {
        this.consumeMessageDirectly = consumeMessageDirectly;
    }

    public boolean isCloneGroupOffset() {
        return cloneGroupOffset;
    }

    public void setCloneGroupOffset(boolean cloneGroupOffset) {
        this.cloneGroupOffset = cloneGroupOffset;
    }

    public boolean isViewBrokerStatsData() {
        return viewBrokerStatsData;
    }

    public void setViewBrokerStatsData(boolean viewBrokerStatsData) {
        this.viewBrokerStatsData = viewBrokerStatsData;
    }

    public boolean isGetBrokerConsumeStats() {
        return getBrokerConsumeStats;
    }

    public void setGetBrokerConsumeStats(boolean getBrokerConsumeStats) {
        this.getBrokerConsumeStats = getBrokerConsumeStats;
    }

    public boolean isQueryConsumeQueue() {
        return queryConsumeQueue;
    }

    public void setQueryConsumeQueue(boolean queryConsumeQueue) {
        this.queryConsumeQueue = queryConsumeQueue;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BorkerAccessControl [permitSendTopic=").append(permitSendTopic).append(", noPermitSendTopic=")
            .append(noPermitSendTopic).append(", permitPullTopic=").append(permitPullTopic)
            .append(", noPermitPullTopic=").append(noPermitPullTopic);
        if (!!sendMessage)
            builder.append(", sendMessage=").append(sendMessage);
        if (!!sendMessageV2)
            builder.append(", sendMessageV2=").append(sendMessageV2);
        if (!sendBatchMessage)
            builder.append(", sendBatchMessage=").append(sendBatchMessage);
        if (!consumerSendMsgBack)
            builder.append(", consumerSendMsgBack=").append(consumerSendMsgBack);
        if (!pullMessage)
            builder.append(", pullMessage=").append(pullMessage);
        if (!queryMessage)
            builder.append(", queryMessage=").append(queryMessage);
        if (!viewMessageById)
            builder.append(", viewMessageById=").append(viewMessageById);
        if (!heartBeat)
            builder.append(", heartBeat=").append(heartBeat);
        if (!unregisterClient)
            builder.append(", unregisterClient=").append(unregisterClient);
        if (!checkClientConfig)
            builder.append(", checkClientConfig=").append(checkClientConfig);
        if (!getConsumerListByGroup)
            builder.append(", getConsumerListByGroup=").append(getConsumerListByGroup);
        if (!updateConsumerOffset)
            builder.append(", updateConsumerOffset=").append(updateConsumerOffset);
        if (!queryConsumerOffset)
            builder.append(", queryConsumerOffset=").append(queryConsumerOffset);
        if (!endTransaction)
            builder.append(", endTransaction=").append(endTransaction);
        if (!updateAndCreateTopic)
            builder.append(", updateAndCreateTopic=").append(updateAndCreateTopic);
        if (!deleteTopicInbroker)
            builder.append(", deleteTopicInbroker=").append(deleteTopicInbroker);
        if (!getAllTopicConfig)
            builder.append(", getAllTopicConfig=").append(getAllTopicConfig);
        if (!updateBrokerConfig)
            builder.append(", updateBrokerConfig=").append(updateBrokerConfig);
        if (!getBrokerConfig)
            builder.append(", getBrokerConfig=").append(getBrokerConfig);
        if (!searchOffsetByTimestamp)
            builder.append(", searchOffsetByTimestamp=").append(searchOffsetByTimestamp);
        if (!getMaxOffset)
            builder.append(", getMaxOffset=").append(getMaxOffset);
        if (!getMixOffset)
            builder.append(", getMixOffset=").append(getMixOffset);
        if (!getEarliestMsgStoretime)
            builder.append(", getEarliestMsgStoretime=").append(getEarliestMsgStoretime);
        if (!getBrokerRuntimeInfo)
            builder.append(", getBrokerRuntimeInfo=").append(getBrokerRuntimeInfo);
        if (!lockBatchMQ)
            builder.append(", lockBatchMQ=").append(lockBatchMQ);
        if (!unlockBatchMQ)
            builder.append(", unlockBatchMQ=").append(unlockBatchMQ);
        if (!updateAndCreteSubscriptiongroup)
            builder.append(", updateAndCreteSubscriptiongroup=").append(updateAndCreteSubscriptiongroup);
        if (!getAllSubscriptiongroupConfig)
            builder.append(", getAllSubscriptiongroupConfig=").append(getAllSubscriptiongroupConfig);
        if (!deleteSubscriptiongroup)
            builder.append(", deleteSubscriptiongroup=").append(deleteSubscriptiongroup);
        if (!getTopicStatsInfo)
            builder.append(", getTopicStatsInfo=").append(getTopicStatsInfo);
        if (!getConsumerConnectionList)
            builder.append(", getConsumerConnectionList=").append(getConsumerConnectionList);
        if (!getProducerConnectionList)
            builder.append(", getProducerConnectionList=").append(getProducerConnectionList);
        if (!getConsumeStats)
            builder.append(", getConsumeStats=").append(getConsumeStats);
        if (!getAllConsumerOffset)
            builder.append(", getAllConsumerOffset=").append(getAllConsumerOffset);
        if (!getAllDelayOffset)
            builder.append(", getAllDelayOffset=").append(getAllDelayOffset);
        if (!invokeBrokerToresetOffset)
            builder.append(", invokeBrokerToresetOffset=").append(invokeBrokerToresetOffset);
        if (!queryTopicConsumByWho)
            builder.append(", queryTopicConsumByWho=").append(queryTopicConsumByWho);
        if (!registerFilterServer)
            builder.append(", registerFilterServer=").append(registerFilterServer);
        if (!queryConsumeTimeSpan)
            builder.append(", queryConsumeTimeSpan=").append(queryConsumeTimeSpan);
        if (!getSystemTopicListFromBroker)
            builder.append(", getSystemTopicListFromBroker=").append(getSystemTopicListFromBroker);
        if (!cleanExpiredConsumequeue)
            builder.append(", cleanExpiredConsumequeue=").append(cleanExpiredConsumequeue);
        if (!getConsumerRunningInfo)
            builder.append(", cleanUnusedTopic=").append(getConsumerRunningInfo);
        if (!getConsumerRunningInfo)
            builder.append(", getConsumerRunningInfo=").append(getConsumerRunningInfo);
        if (!queryCorrectionOffset)
            builder.append(", queryCorrectionOffset=").append(queryCorrectionOffset);
        if (!consumeMessageDirectly)
            builder.append(", consumeMessageDirectly=").append(consumeMessageDirectly);
        if (!cloneGroupOffset)
            builder.append(", cloneGroupOffset=").append(cloneGroupOffset);
        if (!viewBrokerStatsData)
            builder.append(", viewBrokerStatsData=").append(viewBrokerStatsData);
        if (!getBrokerConsumeStats)
            builder.append(", getBrokerConsumeStats=").append(getBrokerConsumeStats);
        if (!queryConsumeQueue)
            builder.append(", queryConsumeQueue=").append(queryConsumeQueue);
        builder.append("]");
        return builder.toString();
    }

}
