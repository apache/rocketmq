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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.BatchConsumeClientDiagnostics;
import apache.rocketmq.v2.BatchConsumeGroupSummary;
import apache.rocketmq.v2.DescribeBatchConsumeDiagnosticsRequest;
import apache.rocketmq.v2.DescribeBatchConsumeDiagnosticsResponse;
import apache.rocketmq.v2.DescribePopReceiptHandlesRequest;
import apache.rocketmq.v2.DescribePopReceiptHandlesResponse;
import apache.rocketmq.v2.MessageModel;
import apache.rocketmq.v2.PopLockView;
import apache.rocketmq.v2.PopReceiptHandleGroupSummary;
import apache.rocketmq.v2.PopReceiptHandleInfo;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroupKey;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.ReceiptHandleProcessor;
import org.apache.rocketmq.proxy.service.receipt.DefaultReceiptHandleManager;

/**
 * RIP-2 M3/M4 diagnostics: POP receipt handle inspection and batch consumption
 * diagnostics, computed from the proxy's own receipt handle tracking (the same
 * state driving invisible-time renewal), so the data is always consistent with
 * what the proxy actually holds for connected consumers.
 */
public class ProxyAdminDiagnosticsSupport {

    private static final int MAX_DIAG_PAGE_SIZE = 100;

    private final DefaultMessagingProcessor messagingProcessor;

    public ProxyAdminDiagnosticsSupport(DefaultMessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
    }

    // ---------------------------------------------------------------------
    // M3: POP receipt handle diagnostics
    // ---------------------------------------------------------------------

    public DescribePopReceiptHandlesResponse describePopReceiptHandles(DescribePopReceiptHandlesRequest request,
        apache.rocketmq.v2.Status ok, apache.rocketmq.v2.Status badRequest) {
        String group = request.getGroup();
        if (group == null || group.isEmpty()) {
            return DescribePopReceiptHandlesResponse.newBuilder().setStatus(badRequest).build();
        }
        String topicFilter = request.getTopic();
        int pageSize = request.getPageSize() <= 0 ? 20 : Math.min(request.getPageSize(), MAX_DIAG_PAGE_SIZE);
        int pageNum = Math.max(request.getPageNum(), 1);

        List<PopReceiptHandleInfo> all = new ArrayList<>();
        PopReceiptHandleGroupSummary.Builder summary = PopReceiptHandleGroupSummary.newBuilder().setGroup(group);
        long totalRenew = 0;
        long totalRenewRetry = 0;
        int expired = 0;
        long now = System.currentTimeMillis();
        Map<String, PopLockView.Builder> lockViews = new LinkedHashMap<>();

        DefaultReceiptHandleManager manager = receiptHandleManager();
        if (manager != null) {
            List<Object[]> collected = new ArrayList<>();
            manager.scanReceiptHandles((groupKey, handle) -> {
                if (!group.equals(handle.getGroup())) {
                    return;
                }
                if (!topicFilter.isEmpty() && !topicFilter.equals(handle.getTopic())) {
                    return;
                }
                collected.add(new Object[] {groupKey, handle});
            });
            for (Object[] pair : collected) {
                ReceiptHandleGroupKey groupKey = (ReceiptHandleGroupKey) pair[0];
                MessageReceiptHandle handle = (MessageReceiptHandle) pair[1];
                PopReceiptHandleInfo info = toHandleInfo(groupKey, handle, now);
                all.add(info);
                totalRenew += handle.getRenewTimes();
                totalRenewRetry += handle.getRenewRetryTimes();
                if (info.getIsExpired()) {
                    expired++;
                }
                String lockKey = handle.getTopic() + ":" + handle.getQueueId();
                lockViews.computeIfAbsent(lockKey, k -> PopLockView.newBuilder()
                    .setGroup(group)
                    .setTopic(handle.getTopic())
                    .setQueueId(handle.getQueueId())
                    .setLockOwner(info.getLockOwner())
                    .setLocked(true));
            }
        }

        summary.setTotalHandles(all.size())
            .setTotalMessages(all.size())
            .setTotalRenewTimes(totalRenew)
            .setTotalRenewRetryTimes(totalRenewRetry)
            .setExpiredHandles(expired)
            .setTotalAckCount(0)
            .setTotalNackCount(0)
            .addAllLockView(buildLockViews(lockViews));

        int fromIndex = Math.min((pageNum - 1) * pageSize, all.size());
        int toIndex = Math.min(fromIndex + pageSize, all.size());
        return DescribePopReceiptHandlesResponse.newBuilder()
            .setStatus(ok)
            .setSummary(summary)
            .addAllHandles(all.subList(fromIndex, toIndex))
            .setTotal(all.size())
            .setPageNum(pageNum)
            .setPageSize(pageSize)
            .build();
    }

    private PopReceiptHandleInfo toHandleInfo(ReceiptHandleGroupKey groupKey, MessageReceiptHandle handle, long now) {
        PopReceiptHandleInfo.Builder builder = PopReceiptHandleInfo.newBuilder()
            .setGroup(handle.getGroup())
            .setTopic(handle.getTopic())
            .setQueueId(handle.getQueueId())
            .setMessageId(handle.getMessageId())
            .setQueueOffset(handle.getQueueOffset())
            .setReconsumeTimes(handle.getReconsumeTimes())
            .setRenewTimes(handle.getRenewTimes())
            .setRenewRetryTimes(handle.getRenewRetryTimes())
            .setConsumeTimestamp(timestamp(handle.getConsumeTimestamp()))
            .setReceiptHandle(handle.getReceiptHandleStr())
            .setLockOwner(clientIdOf(groupKey.getChannel()));
        try {
            ReceiptHandle decoded = ReceiptHandle.decode(handle.getReceiptHandleStr());
            if (decoded != null) {
                builder.setNextVisibleTime(timestamp(decoded.getNextVisibleTime()))
                    .setInvisibleTime(Duration.newBuilder()
                        .setSeconds(decoded.getInvisibleTime() / 1000)
                        .setNanos((int) ((decoded.getInvisibleTime() % 1000) * 1_000_000))
                        .build())
                    .setBrokerName(decoded.getBrokerName() == null ? "" : decoded.getBrokerName())
                    .setIsExpired(decoded.getNextVisibleTime() < now);
            }
        } catch (Throwable t) {
            builder.setIsExpired(false);
        }
        return builder.build();
    }

    private static List<PopLockView> buildLockViews(Map<String, PopLockView.Builder> lockViews) {
        List<PopLockView> result = new ArrayList<>();
        for (PopLockView.Builder builder : lockViews.values()) {
            result.add(builder.build());
        }
        return result;
    }

    // ---------------------------------------------------------------------
    // M4: batch consumption diagnostics
    // ---------------------------------------------------------------------

    public DescribeBatchConsumeDiagnosticsResponse describeBatchConsumeDiagnostics(
        DescribeBatchConsumeDiagnosticsRequest request, apache.rocketmq.v2.Status ok,
        apache.rocketmq.v2.Status badRequest) {
        String group = request.getGroup();
        if (group == null || group.isEmpty()) {
            return DescribeBatchConsumeDiagnosticsResponse.newBuilder().setStatus(badRequest).build();
        }
        String topicFilter = request.getTopic();
        String clientFilter = request.getClientId();
        int pageSize = request.getPageSize() <= 0 ? 20 : Math.min(request.getPageSize(), MAX_DIAG_PAGE_SIZE);
        int pageNum = Math.max(request.getPageNum(), 1);
        long now = System.currentTimeMillis();

        Map<String, BatchConsumeClientDiagnostics.Builder> perClient = new LinkedHashMap<>();
        DefaultReceiptHandleManager manager = receiptHandleManager();
        if (manager != null) {
            manager.scanReceiptHandles((groupKey, handle) -> {
                if (!group.equals(handle.getGroup())) {
                    return;
                }
                if (!topicFilter.isEmpty() && !topicFilter.equals(handle.getTopic())) {
                    return;
                }
                String clientId = clientIdOf(groupKey.getChannel());
                if (!clientFilter.isEmpty() && !clientFilter.equals(clientId)) {
                    return;
                }
                BatchConsumeClientDiagnostics.Builder builder = perClient.computeIfAbsent(clientId, key -> {
                    BatchConsumeClientDiagnostics.Builder newBuilder = BatchConsumeClientDiagnostics.newBuilder()
                        .setClientId(key)
                        .setChannelId(groupKey.getChannel() == null ? "" : groupKey.getChannel().id().asShortText())
                        .setConsumeType("PUSH")
                        .setMessageModel(MessageModel.CLUSTERING);
                    if (groupKey.getChannel() instanceof GrpcClientChannel) {
                        newBuilder.setConnectTime(timestamp(((GrpcClientChannel) groupKey.getChannel()).getConnectTimeMillis()));
                    }
                    return newBuilder;
                });
                builder.setUnackedMessageCount(builder.getUnackedMessageCount() + 1);
                builder.setUnackedHandleCount(builder.getUnackedHandleCount() + 1);
                builder.setTotalRenewTimes(builder.getTotalRenewTimes() + handle.getRenewTimes());
                builder.setTotalRenewRetryTimes(builder.getTotalRenewRetryTimes() + handle.getRenewRetryTimes());
                boolean expiredHandle = false;
                try {
                    ReceiptHandle decoded = ReceiptHandle.decode(handle.getReceiptHandleStr());
                    expiredHandle = decoded != null && decoded.getNextVisibleTime() < now;
                } catch (Throwable ignore) {
                    // undecodable handle: count it as unexpired
                }
                if (expiredHandle) {
                    builder.setExpiredHandleCount(builder.getExpiredHandleCount() + 1);
                }
                builder.putTopicDistribution(handle.getTopic(),
                    builder.getTopicDistributionMap().getOrDefault(handle.getTopic(), 0) + 1);
            });
        }

        BatchConsumeGroupSummary.Builder summary = BatchConsumeGroupSummary.newBuilder()
            .setGroup(group)
            .setTotalClients(perClient.size());
        List<BatchConsumeClientDiagnostics> all = new ArrayList<>();
        for (BatchConsumeClientDiagnostics.Builder builder : perClient.values()) {
            BatchConsumeClientDiagnostics diagnostics = builder.build();
            all.add(diagnostics);
            summary.setTotalUnackedMessages(summary.getTotalUnackedMessages() + diagnostics.getUnackedMessageCount());
            summary.setTotalUnackedHandles(summary.getTotalUnackedHandles() + diagnostics.getUnackedHandleCount());
            summary.setTotalExpiredHandles(summary.getTotalExpiredHandles() + diagnostics.getExpiredHandleCount());
            summary.setTotalRenewTimes(summary.getTotalRenewTimes() + diagnostics.getTotalRenewTimes());
            summary.setTotalRenewRetryTimes(summary.getTotalRenewRetryTimes() + diagnostics.getTotalRenewRetryTimes());
        }

        int fromIndex = Math.min((pageNum - 1) * pageSize, all.size());
        int toIndex = Math.min(fromIndex + pageSize, all.size());
        return DescribeBatchConsumeDiagnosticsResponse.newBuilder()
            .setStatus(ok)
            .setSummary(summary)
            .addAllDiagnostics(all.subList(fromIndex, toIndex))
            .setTotal(all.size())
            .setPageNum(pageNum)
            .setPageSize(pageSize)
            .build();
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    private DefaultReceiptHandleManager receiptHandleManager() {
        if (messagingProcessor == null) {
            return null;
        }
        ReceiptHandleProcessor processor = messagingProcessor.getReceiptHandleProcessor();
        return processor == null ? null : processor.getReceiptHandleManager();
    }

    private static String clientIdOf(Channel channel) {
        if (channel instanceof GrpcClientChannel) {
            return ((GrpcClientChannel) channel).getClientId();
        }
        return channel == null ? "" : channel.id().asShortText();
    }

    private static Timestamp timestamp(long millis) {
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }

    Map<String, Integer> countHandlesByGroup() {
        Map<String, Integer> counts = new HashMap<>();
        DefaultReceiptHandleManager manager = receiptHandleManager();
        if (manager != null) {
            manager.scanReceiptHandles((groupKey, handle) ->
                counts.merge(handle.getGroup(), 1, Integer::sum));
        }
        return counts;
    }
}
