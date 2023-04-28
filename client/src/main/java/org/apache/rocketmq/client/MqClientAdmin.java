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

package org.apache.rocketmq.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public interface MqClientAdmin {
    CompletableFuture<List<MessageExt>> queryMessage(String address, boolean uniqueKeyFlag, boolean decompressBody,
        QueryMessageRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<TopicStatsTable> getTopicStatsInfo(String address,
        GetTopicStatsInfoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String address,
        QueryConsumeTimeSpanRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<Void> updateOrCreateTopic(String address, CreateTopicRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> updateOrCreateSubscriptionGroup(String address, SubscriptionGroupConfig config,
        long timeoutMillis);

    CompletableFuture<Void> deleteTopicInBroker(String address, DeleteTopicRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> deleteTopicInNameserver(String address, DeleteTopicFromNamesrvRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> deleteKvConfig(String address, DeleteKVConfigRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Void> deleteSubscriptionGroup(String address, DeleteSubscriptionGroupRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<Map<MessageQueue, Long>> invokeBrokerToResetOffset(String address,
        ResetOffsetRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<MessageExt> viewMessage(String address, ViewMessageRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<ClusterInfo> getBrokerClusterInfo(String address, long timeoutMillis);

    CompletableFuture<ConsumerConnection> getConsumerConnectionList(String address,
        GetConsumerConnectionListRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<TopicList> queryTopicsByConsumer(String address,
        QueryTopicsByConsumerRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<SubscriptionData> querySubscriptionByConsumer(String address,
        QuerySubscriptionByConsumerRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumeStats> getConsumeStats(String address, GetConsumeStatsRequestHeader requestHeader,
        long timeoutMillis);

    CompletableFuture<GroupList> queryTopicConsumeByWho(String address,
        QueryTopicConsumeByWhoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumerRunningInfo> getConsumerRunningInfo(String address,
        GetConsumerRunningInfoRequestHeader requestHeader, long timeoutMillis);

    CompletableFuture<ConsumeMessageDirectlyResult> consumeMessageDirectly(String address,
        ConsumeMessageDirectlyResultRequestHeader requestHeader, long timeoutMillis);
}
