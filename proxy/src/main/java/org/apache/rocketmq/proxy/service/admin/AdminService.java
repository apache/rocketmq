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
package org.apache.rocketmq.proxy.service.admin;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public interface AdminService extends StartAndShutdown {

    @Override
    default void start() {
    }

    @Override
    default void shutdown() {
    }

    boolean topicExist(String topic);

    boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic, int wQueueNum,
        int rQueueNum, boolean examineTopic, int retryCheckCount);

    boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum, List<BrokerData> curBrokerDataList,
        List<BrokerData> sampleBrokerDataList, boolean examineTopic, int retryCheckCount) throws Exception;

    // =========================================================================
    // Admin gateway: broker-facing queries and mutations.
    //
    // Every call is delegated to the proxy's OWN managed broker client
    // (rocketmq-proxy's MQClientAPIFactory), so the admin surface never opens a
    // direct link to a broker. All methods are asynchronous: the gRPC admin
    // handlers run on the shared gRPC executor and must not block it, and most
    // of them fan out to several brokers at once.
    //
    // A blank {@code topic} on the statistics methods means "every topic of the
    // group", which the broker resolves from its own consumer offset table.
    // =========================================================================

    CompletableFuture<ClusterInfo> getBrokerClusterInfo(long timeoutMillis);

    CompletableFuture<TopicRouteData> getTopicRouteData(String topic);

    CompletableFuture<TopicConfig> getTopicConfig(String brokerAddr, String topic, long timeoutMillis);

    CompletableFuture<TopicStatsTable> getTopicStats(String brokerAddr, String topic, long timeoutMillis);

    CompletableFuture<ConsumeStats> getConsumeStats(String brokerAddr, String group, String topic, long timeoutMillis);

    CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String brokerAddr, String topic, String group,
        long timeoutMillis);

    CompletableFuture<Map<MessageQueue, Long>> resetOffset(String brokerAddr, String topic, String group,
        long timestamp, boolean isForce, long timeoutMillis);

    CompletableFuture<ConsumerConnection> getConsumerConnectionList(String brokerAddr, String group,
        long timeoutMillis);

    CompletableFuture<GroupList> queryTopicConsumeByWho(String brokerAddr, String topic, long timeoutMillis);

    CompletableFuture<TopicList> queryTopicsByConsumer(String brokerAddr, String group, long timeoutMillis);

    CompletableFuture<SubscriptionGroupConfig> getSubscriptionGroupConfig(String brokerAddr, String group,
        long timeoutMillis);

    CompletableFuture<Void> updateSubscriptionGroupConfig(String brokerAddr, SubscriptionGroupConfig config,
        long timeoutMillis);

    CompletableFuture<Void> deleteSubscriptionGroup(String brokerAddr, String group, boolean cleanOffset,
        long timeoutMillis);

    /**
     * Query messages of a topic. When {@code uniqueKey} is true the key is treated as the
     * client-generated unique message id (what the gRPC protocol exposes as {@code message_id});
     * otherwise it is matched against the message keys index.
     *
     * @param decompressBody inflate a compressed body before returning. Callers that render the
     *                       message (QueryMessage) want it inflated; callers that hand the message
     *                       back to a client as-is (VerifyMessage) must not, so the body keeps
     *                       matching the compression flag the client will act on.
     */
    CompletableFuture<List<MessageExt>> queryMessage(String brokerAddr, String topic, String key, int maxNum,
        long beginTimestamp, long endTimestamp, boolean uniqueKey, boolean decompressBody, long timeoutMillis);

    CompletableFuture<MessageExt> viewMessage(String brokerAddr, String topic, long physicalOffset,
        long timeoutMillis);

    CompletableFuture<Long> searchOffsetByTimestamp(String brokerAddr, MessageQueue messageQueue, long timestamp,
        long timeoutMillis);
}
