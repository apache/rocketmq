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

package org.apache.rocketmq.proxy.grpc.admin.model;

import java.util.List;

/**
 * Client detail information model.
 * Extends ClientInstanceInfo with additional diagnostic data.
 * Maps to the ClientDetail proto message.
 */
public class ClientDetailInfo {
    private ClientInstanceInfo clientInstance;
    private ClientSettingsInfo settings;
    private List<HeartbeatRecordInfo> heartbeatHistory;
    private AuthStatusInfo authStatus;
    private ConsumeProgressInfo consumeProgress;
    private NetworkInfoInfo networkInfo;

    public ClientDetailInfo() {
    }

    public ClientInstanceInfo getClientInstance() {
        return clientInstance;
    }

    public void setClientInstance(ClientInstanceInfo clientInstance) {
        this.clientInstance = clientInstance;
    }

    public ClientSettingsInfo getSettings() {
        return settings;
    }

    public void setSettings(ClientSettingsInfo settings) {
        this.settings = settings;
    }

    public List<HeartbeatRecordInfo> getHeartbeatHistory() {
        return heartbeatHistory;
    }

    public void setHeartbeatHistory(List<HeartbeatRecordInfo> heartbeatHistory) {
        this.heartbeatHistory = heartbeatHistory;
    }

    public AuthStatusInfo getAuthStatus() {
        return authStatus;
    }

    public void setAuthStatus(AuthStatusInfo authStatus) {
        this.authStatus = authStatus;
    }

    public ConsumeProgressInfo getConsumeProgress() {
        return consumeProgress;
    }

    public void setConsumeProgress(ConsumeProgressInfo consumeProgress) {
        this.consumeProgress = consumeProgress;
    }

    public NetworkInfoInfo getNetworkInfo() {
        return networkInfo;
    }

    public void setNetworkInfo(NetworkInfoInfo networkInfo) {
        this.networkInfo = networkInfo;
    }

    /**
     * Client settings information.
     */
    public static class ClientSettingsInfo {
        private String subscriptionMode;
        private int receiveBatchSize;
        private long longPollingTimeoutMs;
        private boolean fifo;
        private List<String> subscriptionTopics;
        private List<String> publishingTopics;

        public String getSubscriptionMode() { return subscriptionMode; }
        public void setSubscriptionMode(String subscriptionMode) { this.subscriptionMode = subscriptionMode; }
        public int getReceiveBatchSize() { return receiveBatchSize; }
        public void setReceiveBatchSize(int receiveBatchSize) { this.receiveBatchSize = receiveBatchSize; }
        public long getLongPollingTimeoutMs() { return longPollingTimeoutMs; }
        public void setLongPollingTimeoutMs(long longPollingTimeoutMs) { this.longPollingTimeoutMs = longPollingTimeoutMs; }
        public boolean isFifo() { return fifo; }
        public void setFifo(boolean fifo) { this.fifo = fifo; }
        public List<String> getSubscriptionTopics() { return subscriptionTopics; }
        public void setSubscriptionTopics(List<String> subscriptionTopics) { this.subscriptionTopics = subscriptionTopics; }
        public List<String> getPublishingTopics() { return publishingTopics; }
        public void setPublishingTopics(List<String> publishingTopics) { this.publishingTopics = publishingTopics; }
    }

    /**
     * Heartbeat record information.
     */
    public static class HeartbeatRecordInfo {
        private long timestamp;
        private boolean success;
        private String remark;

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        public String getRemark() { return remark; }
        public void setRemark(String remark) { this.remark = remark; }
    }

    /**
     * Auth status information.
     */
    public static class AuthStatusInfo {
        private boolean authenticated;
        private String username;
        private long lastAuthTime;
        private String failureReason;

        public boolean isAuthenticated() { return authenticated; }
        public void setAuthenticated(boolean authenticated) { this.authenticated = authenticated; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public long getLastAuthTime() { return lastAuthTime; }
        public void setLastAuthTime(long lastAuthTime) { this.lastAuthTime = lastAuthTime; }
        public String getFailureReason() { return failureReason; }
        public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    }

    /**
     * Consume progress information.
     */
    public static class ConsumeProgressInfo {
        private long lag;
        private long latencyMs;
        private List<TopicConsumeProgressInfo> topicProgress;

        public long getLag() { return lag; }
        public void setLag(long lag) { this.lag = lag; }
        public long getLatencyMs() { return latencyMs; }
        public void setLatencyMs(long latencyMs) { this.latencyMs = latencyMs; }
        public List<TopicConsumeProgressInfo> getTopicProgress() { return topicProgress; }
        public void setTopicProgress(List<TopicConsumeProgressInfo> topicProgress) { this.topicProgress = topicProgress; }
    }

    /**
     * Topic consume progress information.
     */
    public static class TopicConsumeProgressInfo {
        private String topic;
        private long lag;
        private long latencyMs;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public long getLag() { return lag; }
        public void setLag(long lag) { this.lag = lag; }
        public long getLatencyMs() { return latencyMs; }
        public void setLatencyMs(long latencyMs) { this.latencyMs = latencyMs; }
    }

    /**
     * Network information.
     */
    public static class NetworkInfoInfo {
        private String localAddress;
        private String remoteAddress;
        private long rttMs;
        private boolean sslEnabled;

        public String getLocalAddress() { return localAddress; }
        public void setLocalAddress(String localAddress) { this.localAddress = localAddress; }
        public String getRemoteAddress() { return remoteAddress; }
        public void setRemoteAddress(String remoteAddress) { this.remoteAddress = remoteAddress; }
        public long getRttMs() { return rttMs; }
        public void setRttMs(long rttMs) { this.rttMs = rttMs; }
        public boolean isSslEnabled() { return sslEnabled; }
        public void setSslEnabled(boolean sslEnabled) { this.sslEnabled = sslEnabled; }
    }
}