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

package org.apache.rocketmq.proxy.common;

import java.util.Map;

/**
 * Diagnostic information for batch consumption, aggregated per client channel.
 * <p>
 * Combines data from ReceiptHandleManager (unacked message statistics)
 * with enrichment from ConsumerManager and GrpcClientSettingsManager.
 * <p>
 * Key diagnostic use cases:
 * - Identify clients with excessive unacked messages (batch too large)
 * - Detect clients with high expired handle counts (timeout issues)
 * - Monitor renewal patterns per client (ChangeInvisibleTime frequency)
 * - Correlate unacked count with configured receiveBatchSize
 */
public class BatchConsumeClientDiagnostics {
    private final String clientId;
    private final String channelId;
    private final int unackedMessageCount;
    private final int unackedHandleCount;
    private final long totalRenewTimes;
    private final long totalRenewRetryTimes;
    private final int expiredHandleCount;
    private final Map<String, Integer> topicDistribution;
    private final String consumeType;
    private final String messageModel;
    private final int receiveBatchSize;
    private final long longPollingTimeoutMs;
    private final long lastRttMs;
    private final long connectTime;

    public BatchConsumeClientDiagnostics(String clientId, String channelId,
        int unackedMessageCount, int unackedHandleCount,
        long totalRenewTimes, long totalRenewRetryTimes, int expiredHandleCount,
        Map<String, Integer> topicDistribution,
        String consumeType, String messageModel,
        int receiveBatchSize, long longPollingTimeoutMs,
        long lastRttMs, long connectTime) {
        this.clientId = clientId;
        this.channelId = channelId;
        this.unackedMessageCount = unackedMessageCount;
        this.unackedHandleCount = unackedHandleCount;
        this.totalRenewTimes = totalRenewTimes;
        this.totalRenewRetryTimes = totalRenewRetryTimes;
        this.expiredHandleCount = expiredHandleCount;
        this.topicDistribution = topicDistribution;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.receiveBatchSize = receiveBatchSize;
        this.longPollingTimeoutMs = longPollingTimeoutMs;
        this.lastRttMs = lastRttMs;
        this.connectTime = connectTime;
    }

    public String getClientId() { return clientId; }
    public String getChannelId() { return channelId; }
    public int getUnackedMessageCount() { return unackedMessageCount; }
    public int getUnackedHandleCount() { return unackedHandleCount; }
    public long getTotalRenewTimes() { return totalRenewTimes; }
    public long getTotalRenewRetryTimes() { return totalRenewRetryTimes; }
    public int getExpiredHandleCount() { return expiredHandleCount; }
    public Map<String, Integer> getTopicDistribution() { return topicDistribution; }
    public String getConsumeType() { return consumeType; }
    public String getMessageModel() { return messageModel; }
    public int getReceiveBatchSize() { return receiveBatchSize; }
    public long getLongPollingTimeoutMs() { return longPollingTimeoutMs; }
    public long getLastRttMs() { return lastRttMs; }
    public long getConnectTime() { return connectTime; }
}