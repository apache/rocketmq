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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse.GroupAccumulation;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
import apache.rocketmq.v2.GetTopicRouteResponse;
import apache.rocketmq.v2.MessageQueueItem;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.QueryTimeSpanResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import com.alibaba.fastjson2.JSON;
import com.google.protobuf.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/**
 * Bridge between the broker's internal wire types ({@code org.apache.rocketmq.remoting.*}) and the
 * gRPC admin contract ({@code apache.rocketmq.v2.*}, generated from rocketmq-apis).
 *
 * <p>This is the only class that knows both worlds: {@link ProxyAdminGrpcService} stays
 * protocol-pure and the admin gateway stays remoting-pure, so neither layer leaks into the other.
 * Converting a whole message body is deliberately NOT done here — the data plane already has
 * {@code GrpcConverter.buildMessage(MessageExt)}, which is reused instead of being duplicated.
 */
final class AdminModelConverter {

    private AdminModelConverter() {
    }

    static Status ok() {
        return Status.newBuilder().setCode(Code.OK).build();
    }

    // -------------------------------------------------------------------------
    // topic
    // -------------------------------------------------------------------------

    static DescribeTopicStatusResponse toTopicStatus(TopicConfig topicConfig, String topic) {
        DescribeTopicStatusResponse.Builder builder = DescribeTopicStatusResponse.newBuilder()
            .setStatus(ok())
            .setTopicMessageType(toMessageType(topicConfig == null ? null : topicConfig.getTopicMessageType()));
        if (topicConfig != null) {
            // a topic creation timestamp is not recorded by the broker, so create_timestamp and
            // tags stay unset rather than being filled with a value that would only look real
            builder.setDescription("readQueueNums=" + topicConfig.getReadQueueNums()
                + ", writeQueueNums=" + topicConfig.getWriteQueueNums()
                + ", perm=" + topicConfig.getPerm());
        } else {
            builder.setDescription("topic config not available for " + topic);
        }
        return builder.build();
    }

    static MessageType toMessageType(TopicMessageType topicMessageType) {
        if (topicMessageType == null) {
            return MessageType.MESSAGE_TYPE_UNSPECIFIED;
        }
        switch (topicMessageType) {
            case NORMAL:
                return MessageType.NORMAL;
            case FIFO:
                return MessageType.FIFO;
            case DELAY:
                return MessageType.DELAY;
            case TRANSACTION:
                return MessageType.TRANSACTION;
            case LITE:
                return MessageType.LITE;
            case UNSPECIFIED:
            case MIXED:
            case PRIORITY:
            default:
                // MessageType.UNRECOGNIZED cannot be set on a builder, so anything this proxy does
                // not understand degrades to UNSPECIFIED instead of failing the whole RPC
                return MessageType.MESSAGE_TYPE_UNSPECIFIED;
        }
    }

    static GetTopicRouteResponse toTopicRoute(TopicRouteData topicRouteData) {
        return GetTopicRouteResponse.newBuilder()
            .setStatus(ok())
            .setTopicRouteData(topicRouteData == null ? "{}" : JSON.toJSONString(topicRouteData))
            .build();
    }

    // -------------------------------------------------------------------------
    // consume time span
    // -------------------------------------------------------------------------

    /**
     * The broker already computes min/max/consume timestamps and the delay per queue
     * (RequestCode.QUERY_CONSUME_TIME_SPAN), so the response maps 1:1 and no offset table lookup
     * is needed here.
     */
    static QueryTimeSpanResponse toQueryTimeSpan(List<QueueTimeSpan> queueTimeSpans) {
        QueryTimeSpanResponse.Builder builder = QueryTimeSpanResponse.newBuilder().setStatus(ok());
        if (queueTimeSpans == null) {
            return builder.build();
        }
        for (QueueTimeSpan span : queueTimeSpans) {
            if (span == null || span.getMessageQueue() == null) {
                continue;
            }
            long delay = span.getDelayTime();
            builder.addQueueTimeSpanList(QueryTimeSpanResponse.QueueTimeSpan.newBuilder()
                .setMessageQueue(toMessageQueue(span.getMessageQueue()))
                .setMinTimestamp(span.getMinTimeStamp())
                .setMaxTimestamp(span.getMaxTimeStamp())
                .setConsumeTimestamp(span.getConsumeTimeStamp())
                .setDelayTimeMs(Math.max(delay, 0L))
                .build());
        }
        return builder.build();
    }

    static apache.rocketmq.v2.MessageQueue toMessageQueue(org.apache.rocketmq.common.message.MessageQueue mq) {
        return apache.rocketmq.v2.MessageQueue.newBuilder()
            .setTopic(Resource.newBuilder().setName(mq.getTopic()).build())
            .setBroker(apache.rocketmq.v2.Broker.newBuilder().setName(mq.getBrokerName()).build())
            .setId(mq.getQueueId())
            .build();
    }

    // -------------------------------------------------------------------------
    // accumulation
    // -------------------------------------------------------------------------

    /**
     * Aggregates consume stats collected from every broker of the cluster.
     *
     * <p>Per the contract {@code accumulation = inflight + ready}, where inflight counts messages
     * already delivered but not yet acknowledged ({@code pullOffset - consumerOffset}) and ready
     * counts messages still waiting ({@code brokerOffset - pullOffset}). Pop retry topics are
     * folded into their normal topic so a caller asking about a topic sees its whole backlog; the
     * pull retry topic is counted in the group total but kept out of the per-topic map because it
     * is not a user-visible topic.
     *
     * @param statsByBroker consume stats per broker address; a null topic filter means the broker
     *                      returned every topic the group has offsets for
     * @param group         consumer group, needed to recognise its retry topics
     */
    static AccumulationResult toAccumulation(Map<String, ConsumeStats> statsByBroker, String group) {
        long totalDiff = 0L;
        long totalInflight = 0L;
        long earliestLastConsume = Long.MAX_VALUE;
        Map<String, long[]> byTopic = new HashMap<>();

        String pullRetryTopic = MixAll.getRetryTopic(group);
        for (ConsumeStats stats : statsByBroker.values()) {
            if (stats == null || stats.getOffsetTable() == null) {
                continue;
            }
            for (Map.Entry<org.apache.rocketmq.common.message.MessageQueue, OffsetWrapper> entry :
                stats.getOffsetTable().entrySet()) {
                org.apache.rocketmq.common.message.MessageQueue mq = entry.getKey();
                OffsetWrapper wrapper = entry.getValue();
                if (mq == null || wrapper == null) {
                    continue;
                }
                long diff = Math.max(wrapper.getBrokerOffset() - wrapper.getConsumerOffset(), 0L);
                long inflight = Math.max(wrapper.getPullOffset() - wrapper.getConsumerOffset(), 0L);
                if (inflight > diff) {
                    inflight = diff;
                }
                totalDiff += diff;
                totalInflight += inflight;
                if (diff > 0 && wrapper.getLastTimestamp() > 0) {
                    earliestLastConsume = Math.min(earliestLastConsume, wrapper.getLastTimestamp());
                }

                String topic = mq.getTopic();
                if (pullRetryTopic.equals(topic)) {
                    continue;
                }
                String userTopic = KeyBuilder.parseNormalTopic(topic, group);
                long[] acc = byTopic.computeIfAbsent(userTopic, k -> new long[3]);
                acc[0] += diff;
                acc[1] += inflight;
                if (diff > 0 && wrapper.getLastTimestamp() > 0 && (acc[2] == 0L || wrapper.getLastTimestamp() < acc[2])) {
                    acc[2] = wrapper.getLastTimestamp();
                }
            }
        }

        AccumulationResult result = new AccumulationResult(buildAccumulation(totalDiff, totalInflight, earliestLastConsume));
        for (Map.Entry<String, long[]> entry : byTopic.entrySet()) {
            long[] acc = entry.getValue();
            result.byTopic.put(entry.getKey(), buildAccumulation(acc[0], acc[1], acc[2]));
        }
        return result;
    }

    private static GroupAccumulation buildAccumulation(long accumulation, long inflight, long lastConsumeTimestamp) {
        GroupAccumulation.Builder builder = GroupAccumulation.newBuilder()
            .setAccumulation(accumulation)
            .setInflightMessages(inflight)
            .setReadyMessages(Math.max(accumulation - inflight, 0L));
        if (lastConsumeTimestamp > 0 && lastConsumeTimestamp != Long.MAX_VALUE) {
            builder.setLastConsumeTimestamp(lastConsumeTimestamp);
            if (accumulation > 0) {
                long delayMillis = Math.max(System.currentTimeMillis() - lastConsumeTimestamp, 0L);
                builder.setDeliverDelayTime(Duration.newBuilder()
                    .setSeconds(delayMillis / 1000L)
                    .setNanos((int) (delayMillis % 1000L) * 1_000_000)
                    .build());
            }
        }
        return builder.build();
    }

    static final class AccumulationResult {
        final GroupAccumulation total;
        final Map<String, GroupAccumulation> byTopic = new HashMap<>();

        private AccumulationResult(GroupAccumulation total) {
            this.total = total;
        }
    }

    // -------------------------------------------------------------------------
    // clients
    // -------------------------------------------------------------------------

    /**
     * Builds a {@code ClientInfo} from a broker-side connection. The broker sees consumers that
     * registered through any proxy of the cluster, which is why the admin surface prefers this
     * over scanning only the local gRPC channels.
     */
    static apache.rocketmq.v2.ClientInfo toClientInfo(Connection connection,
        apache.rocketmq.v2.MessageModel messageModel) {
        apache.rocketmq.v2.ClientInfo.Builder builder = apache.rocketmq.v2.ClientInfo.newBuilder()
            .setClientId(connection.getClientId() == null ? "" : connection.getClientId());
        if (connection.getLanguage() != null) {
            builder.setLanguage(connection.getLanguage().name());
        }
        builder.setVersion(String.valueOf(connection.getVersion()));
        String clientAddr = connection.getClientAddr();
        if (clientAddr != null && !clientAddr.isEmpty()) {
            // clientAddr is "ip:port"; egress_ip is documented as the address observed by the server
            int idx = clientAddr.lastIndexOf('@');
            String addr = idx >= 0 ? clientAddr.substring(idx + 1) : clientAddr;
            int colon = addr.lastIndexOf(':');
            builder.setEgressIp(colon > 0 ? addr.substring(0, colon) : addr);
            builder.setHostname(idx >= 0 ? clientAddr.substring(0, idx) : "");
        }
        if (messageModel != null) {
            builder.setMessageModel(messageModel);
        }
        return builder.build();
    }

    static apache.rocketmq.v2.MessageModel toMessageModel(
        org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel messageModel) {
        if (messageModel == null) {
            return apache.rocketmq.v2.MessageModel.MESSAGE_MODEL_UNSPECIFIED;
        }
        switch (messageModel) {
            case BROADCASTING:
                return apache.rocketmq.v2.MessageModel.BROADCASTING;
            case CLUSTERING:
            default:
                return apache.rocketmq.v2.MessageModel.CLUSTERING;
        }
    }

    static apache.rocketmq.v2.FilterExpression toFilterExpression(String expressionType, String expression) {
        apache.rocketmq.v2.FilterType type = ExpressionType.SQL92.equals(expressionType)
            ? apache.rocketmq.v2.FilterType.SQL : apache.rocketmq.v2.FilterType.TAG;
        return apache.rocketmq.v2.FilterExpression.newBuilder()
            .setType(type)
            .setExpression(expression == null ? "" : expression)
            .build();
    }

    /**
     * Full mapping of a remoting client's running info. A gRPC v2 client can only report its
     * thread stack (the telemetry contract has no reply message carrying the other three fields),
     * so this is reached only for remoting clients.
     */
    static apache.rocketmq.v2.ConsumerRunningInfo toConsumerRunningInfo(
        org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo runningInfo) {
        apache.rocketmq.v2.ConsumerRunningInfo.Builder builder = apache.rocketmq.v2.ConsumerRunningInfo.newBuilder();
        if (runningInfo == null) {
            return builder.build();
        }
        if (runningInfo.getProperties() != null) {
            for (String name : runningInfo.getProperties().stringPropertyNames()) {
                builder.putProperties(name, runningInfo.getProperties().getProperty(name));
            }
        }
        if (runningInfo.getSubscriptionSet() != null) {
            for (org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData data :
                runningInfo.getSubscriptionSet()) {
                builder.putSubscriptions(data.getTopic(), toFilterExpression(data.getExpressionType(), data.getSubString()));
            }
        }
        if (runningInfo.getMqTable() != null) {
            for (Map.Entry<org.apache.rocketmq.common.message.MessageQueue,
                org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo> entry : runningInfo.getMqTable().entrySet()) {
                org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo info = entry.getValue();
                if (info == null) {
                    continue;
                }
                builder.addMessageQueueTable(MessageQueueItem.newBuilder()
                    .setMessageQueue(toMessageQueue(entry.getKey()))
                    .setProcessQueueInfo(apache.rocketmq.v2.ProcessQueueInfo.newBuilder()
                        .setCommitOffset(info.getCommitOffset())
                        .setCachedMsgMinOffset(info.getCachedMsgMinOffset())
                        .setCachedMsgMaxOffset(info.getCachedMsgMaxOffset())
                        .setCachedMsgCount(info.getCachedMsgCount())
                        .setCachedMsgSizeInMib(info.getCachedMsgSizeInMiB())
                        .setTransactionMsgMinOffset(info.getTransactionMsgMinOffset())
                        .setTransactionMsgMaxOffset(info.getTransactionMsgMaxOffset())
                        .setTransactionMsgCount(info.getTransactionMsgCount())
                        .setLocked(info.isLocked())
                        .setTryUnlockTimes(info.getTryUnlockTimes())
                        .setLastLockTimestamp(info.getLastLockTimestamp())
                        .setDropped(info.isDroped())
                        .setLastPullTimestamp(info.getLastPullTimestamp())
                        .setLastConsumeTimestamp(info.getLastConsumeTimestamp())
                        .build())
                    .build());
            }
        }
        if (runningInfo.getStatusTable() != null) {
            for (Map.Entry<String, org.apache.rocketmq.remoting.protocol.body.ConsumeStatus> entry :
                runningInfo.getStatusTable().entrySet()) {
                org.apache.rocketmq.remoting.protocol.body.ConsumeStatus status = entry.getValue();
                if (status == null) {
                    continue;
                }
                builder.putConsumeStatusTable(entry.getKey(), apache.rocketmq.v2.ConsumeStatus.newBuilder()
                    .setReceiveRt(status.getPullRT())
                    .setReceiveTps(status.getPullTPS())
                    .setConsumeRt(status.getConsumeRT())
                    .setConsumeOkTps(status.getConsumeOKTPS())
                    .setConsumeFailedTps(status.getConsumeFailedTPS())
                    .setConsumeFailedMsgs(status.getConsumeFailedMsgs())
                    .build());
            }
        }
        return builder.build();
    }
}
