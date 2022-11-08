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

package org.apache.rocketmq.proxy.service.sysmessage;

import java.util.Set;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class HeartbeatSyncerData {
    private HeartbeatType heartbeatType;
    private String clientId;
    private LanguageCode language;
    private int version;
    private long lastUpdateTimestamp = System.currentTimeMillis();
    private Set<SubscriptionData> subscriptionDataSet;
    private String group;
    private ConsumeType consumeType;
    private MessageModel messageModel;
    private ConsumeFromWhere consumeFromWhere;
    private String connectProxyIp;
    private String channelData;
    private String channelExtendAttribute;

    public HeartbeatSyncerData(HeartbeatType heartbeatType, String clientId,
        LanguageCode language, int version, String group,
        ConsumeType consumeType, MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere, String connectProxyIp,
        String channelData, String channelExtendAttribute) {
        this.heartbeatType = heartbeatType;
        this.clientId = clientId;
        this.language = language;
        this.version = version;
        this.group = group;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
        this.connectProxyIp = connectProxyIp;
        this.channelData = channelData;
        this.channelExtendAttribute = channelExtendAttribute;
    }

    public HeartbeatType getHeartbeatType() {
        return heartbeatType;
    }

    public void setHeartbeatType(HeartbeatType heartbeatType) {
        this.heartbeatType = heartbeatType;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public Set<SubscriptionData> getSubscriptionDataSet() {
        return subscriptionDataSet;
    }

    public void setSubscriptionDataSet(
        Set<SubscriptionData> subscriptionDataSet) {
        this.subscriptionDataSet = subscriptionDataSet;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public String getConnectProxyIp() {
        return connectProxyIp;
    }

    public void setConnectProxyIp(String connectProxyIp) {
        this.connectProxyIp = connectProxyIp;
    }

    public String getChannelData() {
        return channelData;
    }

    public void setChannelData(String channelData) {
        this.channelData = channelData;
    }

    public String getChannelExtendAttribute() {
        return channelExtendAttribute;
    }

    public void setChannelExtendAttribute(String channelExtendAttribute) {
        this.channelExtendAttribute = channelExtendAttribute;
    }
}
