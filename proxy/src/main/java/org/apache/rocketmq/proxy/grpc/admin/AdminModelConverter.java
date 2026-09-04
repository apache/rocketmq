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

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
import apache.rocketmq.v2.GetTopicRouteResponse;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.QueryTimeSpanResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SystemProperties;
import com.alibaba.fastjson.JSON;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;

/**
 * Bridge that translates between the broker's internal wire types
 * ({@code org.apache.rocketmq.remoting.*}) and the RIP-2 gRPC protocol
 * ({@code apache.rocketmq.v2.*}, generated from rocketmq-apis).
 *
 * <p>This is the ONLY class that imports both worlds. The gRPC admin service
 * ({@link ProxyAdminGrpcService}) stays protocol-pure (v2 only); the broker
 * gateway ({@code DefaultAdminService}) stays remoting-pure. Keeping the
 * conversion here ensures neither layer leaks into the other.
 */
final class AdminModelConverter {

    private AdminModelConverter() {
    }

    private static Status ok() {
        return Status.newBuilder().setCode(Code.OK).build();
    }

    static DescribeGroupAccumulationResponse.GroupAccumulation toGroupAccumulation(
        AdminService adminService, String brokerAddr, String group, String topic, long timeoutMillis) throws Exception {
        ConsumeStats consumeStats = adminService.fetchConsumeStats(brokerAddr, group, topic, timeoutMillis);
        long total = 0;
        if (consumeStats != null && consumeStats.getOffsetTable() != null) {
            for (OffsetWrapper wrapper : consumeStats.getOffsetTable().values()) {
                long diff = wrapper.getBrokerOffset() - wrapper.getConsumerOffset();
                if (diff > 0) {
                    total += diff;
                }
            }
        }
        return DescribeGroupAccumulationResponse.GroupAccumulation.newBuilder()
            .setAccumulation(total)
            .setReadyMessages(total)
            .build();
    }

    /**
     * RIP-2 fix: aggregate accumulation across ALL brokers hosting the topic, deduplicating
     * by message queue, instead of only querying the first broker of the route.
     */
    static DescribeGroupAccumulationResponse.GroupAccumulation toGroupAccumulationMultiBroker(
        AdminService adminService, java.util.Collection<String> brokerAddrs, String group, String topic,
        long timeoutMillis) throws Exception {
        long total = 0;
        java.util.Set<org.apache.rocketmq.common.message.MessageQueue> counted = new java.util.HashSet<>();
        for (String brokerAddr : brokerAddrs) {
            ConsumeStats consumeStats = adminService.fetchConsumeStats(brokerAddr, group, topic, timeoutMillis);
            if (consumeStats == null || consumeStats.getOffsetTable() == null) {
                continue;
            }
            for (java.util.Map.Entry<org.apache.rocketmq.common.message.MessageQueue, OffsetWrapper> entry :
                consumeStats.getOffsetTable().entrySet()) {
                if (!counted.add(entry.getKey())) {
                    continue;
                }
                long diff = entry.getValue().getBrokerOffset() - entry.getValue().getConsumerOffset();
                if (diff > 0) {
                    total += diff;
                }
            }
        }
        return DescribeGroupAccumulationResponse.GroupAccumulation.newBuilder()
            .setAccumulation(total)
            .setReadyMessages(total)
            .build();
    }

    static QueryTimeSpanResponse toQueryTimeSpan(
        AdminService adminService, String brokerAddr, String group, String topic,
        MessageQueueView mqv, long timeoutMillis) throws Exception {
        ConsumeStats consumeStats = adminService.fetchConsumeStats(brokerAddr, group, topic, timeoutMillis);
        QueryTimeSpanResponse.Builder builder = QueryTimeSpanResponse.newBuilder().setStatus(ok());
        if (mqv != null && mqv.getReadSelector() != null) {
            for (AddressableMessageQueue mq : mqv.getReadSelector().getQueues()) {
                String queueBrokerAddr = mq.getBrokerAddr();
                if (queueBrokerAddr == null || queueBrokerAddr.isEmpty()) {
                    queueBrokerAddr = brokerAddr;
                }
                long minStoretime = adminService.getEarliestMsgStoretime(queueBrokerAddr, mq, timeoutMillis);
                QueryTimeSpanResponse.QueueTimeSpan.Builder span = QueryTimeSpanResponse.QueueTimeSpan.newBuilder()
                    .setMessageQueue(toMessageQueue(mq))
                    .setMinTimestamp(minStoretime);
                // RIP-2 fix: report the real last consume timestamp from the offset table
                // (fallback to minStoretime when the queue has never been consumed).
                OffsetWrapper wrapper = consumeStats != null && consumeStats.getOffsetTable() != null
                    ? consumeStats.getOffsetTable().get(mq) : null;
                if (wrapper != null && wrapper.getLastTimestamp() > 0) {
                    span.setConsumeTimestamp(wrapper.getLastTimestamp());
                } else {
                    span.setConsumeTimestamp(minStoretime);
                }
                builder.addQueueTimeSpanList(span.build());
            }
        }
        return builder.build();
    }

    static GetTopicRouteResponse toTopicRoute(AdminService adminService, String topic) throws Exception {
        TopicRouteData topicRouteData = adminService.getTopicRouteData(topic);
        String json = topicRouteData == null ? "{}" : JSON.toJSONString(topicRouteData);
        return GetTopicRouteResponse.newBuilder()
            .setStatus(ok())
            .setTopicRouteData(json)
            .build();
    }

    static DescribeTopicStatusResponse toTopicStatus(AdminService adminService, String brokerAddr, String topic,
        long timeoutMillis) throws Exception {
        TopicConfigAndQueueMapping topicConfig = adminService.getTopicConfig(brokerAddr, topic, timeoutMillis);
        DescribeTopicStatusResponse.Builder builder = DescribeTopicStatusResponse.newBuilder().setStatus(ok());
        if (topicConfig != null) {
            builder.setTopicMessageType(MessageType.MESSAGE_TYPE_UNSPECIFIED);
            builder.setDescription("topic=" + topicConfig.getTopicName()
                + " readQueues=" + topicConfig.getReadQueueNums()
                + " writeQueues=" + topicConfig.getWriteQueueNums()
                + " perm=" + topicConfig.getPerm());
        }
        return builder.build();
    }

    static Message toMessage(MessageExt ext) {
        if (ext == null) {
            return null;
        }
        List<String> keys = new ArrayList<>();
        if (ext.getKeys() != null && !ext.getKeys().isEmpty()) {
            for (String k : ext.getKeys().split("\\s+")) {
                if (!k.isEmpty()) {
                    keys.add(k);
                }
            }
        }
        SystemProperties.Builder sp = SystemProperties.newBuilder()
            .setMessageId(ext.getMsgId() == null ? "" : ext.getMsgId())
            .setTag(ext.getTags() == null ? "" : ext.getTags());
        if (!keys.isEmpty()) {
            sp.addAllKeys(keys);
        }
        return Message.newBuilder()
            .setTopic(Resource.newBuilder().setName(ext.getTopic()).build())
            .setSystemProperties(sp.build())
            .setBody(ByteString.copyFrom(ext.getBody() == null ? new byte[0] : ext.getBody()))
            .build();
    }

    static apache.rocketmq.v2.MessageQueue toMessageQueue(org.apache.rocketmq.common.message.MessageQueue mq) {
        return apache.rocketmq.v2.MessageQueue.newBuilder()
            .setTopic(Resource.newBuilder().setName(mq.getTopic()).build())
            .setBroker(Broker.newBuilder().setName(mq.getBrokerName()).build())
            .setId(mq.getQueueId())
            .build();
    }
}
