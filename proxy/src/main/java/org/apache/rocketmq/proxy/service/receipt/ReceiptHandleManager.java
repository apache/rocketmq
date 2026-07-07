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

package org.apache.rocketmq.proxy.service.receipt;

import io.netty.channel.Channel;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.proxy.common.BatchConsumeGroupSummary;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.PopReceiptHandleInfo;
import org.apache.rocketmq.proxy.common.PopReceiptHandleGroupSummary;
import org.apache.rocketmq.proxy.common.ProxyContext;

public interface ReceiptHandleManager {
    void addReceiptHandle(ProxyContext context, Channel channel, String group, String msgID, MessageReceiptHandle messageReceiptHandle);

    MessageReceiptHandle removeReceiptHandle(ProxyContext context, Channel channel, String group, String msgID, String receiptHandle);

    int getUnackedMessageCount(ProxyContext context, Channel channel, String group);

    /**
     * Query POP receipt handles for diagnostics.
     * <p>
     * Scans all receipt handle groups matching the given consumer group,
     * collects diagnostic information for each unacked message handle,
     * and returns a summary with paginated handle details.
     *
     * @param group    consumer group name (required)
     * @param topic    optional topic filter, null or empty means no filter
     * @param pageNum  page number starting from 1
     * @param pageSize page size, max 100
     * @return diagnostic result containing summary and paginated handle details
     */
    PopReceiptHandleDiagnosticResult describePopReceiptHandles(String group, String topic, int pageNum, int pageSize);

    /**
     * Result of POP receipt handle diagnostic query.
     */
    class PopReceiptHandleDiagnosticResult {
        private final PopReceiptHandleGroupSummary summary;
        private final List<PopReceiptHandleInfo> handles;
        private final long total;
        private final int pageNum;
        private final int pageSize;

        public PopReceiptHandleDiagnosticResult(PopReceiptHandleGroupSummary summary, List<PopReceiptHandleInfo> handles,
            long total, int pageNum, int pageSize) {
            this.summary = summary;
            this.handles = handles;
            this.total = total;
            this.pageNum = pageNum;
            this.pageSize = pageSize;
        }

        public PopReceiptHandleGroupSummary getSummary() {
            return summary;
        }

        public List<PopReceiptHandleInfo> getHandles() {
            return handles;
        }

        public long getTotal() {
            return total;
        }

        public int getPageNum() {
            return pageNum;
        }

        public int getPageSize() {
            return pageSize;
        }
    }

    /**
     * Query batch consumption diagnostics, aggregated per client channel.
     * <p>
     * Scans all receipt handle groups matching the given consumer group,
     * aggregates unacked message statistics per Channel, and returns
     * a summary with paginated per-client diagnostic details.
     * <p>
     * This method only returns raw data from ReceiptHandleManager.
     * The caller (DefaultProxyAdminClientService) is responsible for
     * enriching with GrpcChannelManager/GrpcClientSettingsManager data.
     *
     * @param group    consumer group name (required)
     * @param topic    optional topic filter, null or empty means no filter
     * @param pageNum  page number starting from 1
     * @param pageSize page size, max 100
     * @return diagnostic result containing summary and paginated per-channel diagnostics
     */
    BatchConsumeDiagnosticResult describeBatchConsumeDiagnostics(String group, String topic, int pageNum, int pageSize);

    /**
     * Per-channel raw data for batch consumption diagnostics.
     * Used internally to transfer data from ReceiptHandleManager to DefaultProxyAdminClientService
     * for enrichment with gRPC channel/settings data.
     */
    class ChannelBatchConsumeData {
        private final Channel channel;
        private final int unackedMessageCount;
        private final int unackedHandleCount;
        private final long totalRenewTimes;
        private final long totalRenewRetryTimes;
        private final int expiredHandleCount;
        private final Map<String, Integer> topicDistribution;

        public ChannelBatchConsumeData(Channel channel, int unackedMessageCount, int unackedHandleCount,
            long totalRenewTimes, long totalRenewRetryTimes, int expiredHandleCount,
            Map<String, Integer> topicDistribution) {
            this.channel = channel;
            this.unackedMessageCount = unackedMessageCount;
            this.unackedHandleCount = unackedHandleCount;
            this.totalRenewTimes = totalRenewTimes;
            this.totalRenewRetryTimes = totalRenewRetryTimes;
            this.expiredHandleCount = expiredHandleCount;
            this.topicDistribution = topicDistribution;
        }

        public Channel getChannel() { return channel; }
        public int getUnackedMessageCount() { return unackedMessageCount; }
        public int getUnackedHandleCount() { return unackedHandleCount; }
        public long getTotalRenewTimes() { return totalRenewTimes; }
        public long getTotalRenewRetryTimes() { return totalRenewRetryTimes; }
        public int getExpiredHandleCount() { return expiredHandleCount; }
        public Map<String, Integer> getTopicDistribution() { return topicDistribution; }
    }

    /**
     * Result of batch consumption diagnostic query.
     */
    class BatchConsumeDiagnosticResult {
        private final BatchConsumeGroupSummary summary;
        private final List<ChannelBatchConsumeData> channelData;
        private final long total;
        private final int pageNum;
        private final int pageSize;

        public BatchConsumeDiagnosticResult(BatchConsumeGroupSummary summary,
            List<ChannelBatchConsumeData> channelData, long total, int pageNum, int pageSize) {
            this.summary = summary;
            this.channelData = channelData;
            this.total = total;
            this.pageNum = pageNum;
            this.pageSize = pageSize;
        }

        public BatchConsumeGroupSummary getSummary() { return summary; }
        public List<ChannelBatchConsumeData> getChannelData() { return channelData; }
        public long getTotal() { return total; }
        public int getPageNum() { return pageNum; }
        public int getPageSize() { return pageSize; }
    }
}
