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

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.impl.admin.MqClientAdminImpl;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class DefaultAdminService implements AdminService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final long ROUTE_TIMEOUT_MILLIS = Duration.ofSeconds(3).toMillis();

    private final MQClientAPIFactory mqClientAPIFactory;

    /**
     * A few broker requests only have a blocking client method (topic config, subscription group
     * config, search offset, nameserver route). The admin gateway promises non-blocking calls so
     * that a handler fanning out to N brokers never parks a gRPC executor thread, so those are
     * lifted onto this pool. Daemon threads: the pool lives as long as the proxy process and must
     * not keep it alive on shutdown.
     */
    private final ExecutorService blockingCallExecutor = Executors.newFixedThreadPool(
        Math.max(4, Runtime.getRuntime().availableProcessors()), new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "AdminBlockingCall_" + seq.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });

    public DefaultAdminService(MQClientAPIFactory mqClientAPIFactory) {
        this.mqClientAPIFactory = mqClientAPIFactory;
    }

    @Override
    public boolean topicExist(String topic) {
        boolean topicExist;
        TopicRouteData topicRouteData;
        try {
            topicRouteData = this.getTopicRouteDataDirectlyFromNameServer(topic);
            topicExist = topicRouteData != null;
        } catch (Throwable e) {
            topicExist = false;
        }

        return topicExist;
    }

    @Override
    public boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic, int wQueueNum,
        int rQueueNum, boolean examineTopic, int retryCheckCount) {
        TopicRouteData curTopicRouteData = new TopicRouteData();
        try {
            curTopicRouteData = this.getTopicRouteDataDirectlyFromNameServer(createTopic);
        } catch (Exception e) {
            if (!TopicRouteHelper.isTopicNotExistError(e)) {
                log.error("get cur topic route {} failed.", createTopic, e);
                return false;
            }
        }

        TopicRouteData sampleTopicRouteData = null;
        try {
            sampleTopicRouteData = this.getTopicRouteDataDirectlyFromNameServer(sampleTopic);
        } catch (Exception e) {
            log.error("create topic {} failed.", createTopic, e);
            return false;
        }

        if (sampleTopicRouteData == null || sampleTopicRouteData.getBrokerDatas().isEmpty()) {
            return false;
        }

        try {
            return this.createTopicOnBroker(createTopic, wQueueNum, rQueueNum, curTopicRouteData.getBrokerDatas(),
                sampleTopicRouteData.getBrokerDatas(), examineTopic, retryCheckCount);
        } catch (Exception e) {
            log.error("create topic {} failed.", createTopic, e);
        }
        return false;
    }

    @Override
    public boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum, List<BrokerData> curBrokerDataList,
        List<BrokerData> sampleBrokerDataList, boolean examineTopic, int retryCheckCount) throws Exception {
        Set<String> curBrokerAddr = new HashSet<>();
        if (curBrokerDataList != null) {
            for (BrokerData brokerData : curBrokerDataList) {
                curBrokerAddr.add(brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
            }
        }

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setWriteQueueNums(wQueueNum);
        topicConfig.setReadQueueNums(rQueueNum);
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);

        for (BrokerData brokerData : sampleBrokerDataList) {
            String addr = brokerData.getBrokerAddrs() == null ? null : brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (addr == null) {
                continue;
            }
            if (curBrokerAddr.contains(addr)) {
                continue;
            }

            try {
                this.getClient().createTopic(addr, TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, topicConfig, ROUTE_TIMEOUT_MILLIS);
            } catch (Exception e) {
                log.error("create topic on broker failed. topic:{}, broker:{}", topicConfig, addr, e);
            }
        }

        if (examineTopic) {
            // examine topic exist.
            int count = retryCheckCount;
            while (count-- > 0) {
                if (this.topicExist(topic)) {
                    return true;
                }
            }
        } else {
            return true;
        }
        return false;
    }

    protected TopicRouteData getTopicRouteDataDirectlyFromNameServer(String topic) throws Exception {
        return this.getClient().getTopicRouteInfoFromNameServer(topic, ROUTE_TIMEOUT_MILLIS);
    }

    protected MQClientAPIExt getClient() {
        return this.mqClientAPIFactory.getClient();
    }

    protected MqClientAdminImpl getAdmin() {
        return this.getClient().getMqClientAdmin();
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
        this.blockingCallExecutor.shutdownNow();
    }

    private <T> CompletableFuture<T> supplyBlocking(java.util.function.Supplier<T> call) {
        return CompletableFuture.supplyAsync(call, blockingCallExecutor);
    }

    // =========================================================================
    // Admin gateway: broker-facing queries and mutations.
    // Every call goes through the proxy's OWN managed broker client.
    // =========================================================================

    @Override
    public CompletableFuture<ClusterInfo> getBrokerClusterInfo(long timeoutMillis) {
        // a null address makes the remoting client talk to the nameserver
        return this.getAdmin().getBrokerClusterInfo(null, timeoutMillis);
    }

    @Override
    public CompletableFuture<TopicRouteData> getTopicRouteData(String topic) {
        return supplyBlocking(() -> {
            try {
                return this.getTopicRouteDataDirectlyFromNameServer(topic);
            } catch (Exception e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<TopicConfig> getTopicConfig(String brokerAddr, String topic, long timeoutMillis) {
        return supplyBlocking(() -> {
            try {
                return this.getClient().getTopicConfig(brokerAddr, topic, timeoutMillis);
            } catch (Exception e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<TopicStatsTable> getTopicStats(String brokerAddr, String topic, long timeoutMillis) {
        GetTopicStatsInfoRequestHeader header = new GetTopicStatsInfoRequestHeader();
        header.setTopic(topic);
        return this.getAdmin().getTopicStatsInfo(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<ConsumeStats> getConsumeStats(String brokerAddr, String group, String topic,
        long timeoutMillis) {
        GetConsumeStatsRequestHeader header = new GetConsumeStatsRequestHeader();
        header.setConsumerGroup(group);
        // a blank topic tells the broker to collect stats over every topic the group has offsets for
        header.setTopic(topic == null ? "" : topic);
        return this.getAdmin().getConsumeStats(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String brokerAddr, String topic, String group,
        long timeoutMillis) {
        QueryConsumeTimeSpanRequestHeader header = new QueryConsumeTimeSpanRequestHeader();
        header.setTopic(topic);
        header.setGroup(group);
        return this.getAdmin().queryConsumeTimeSpan(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<Map<MessageQueue, Long>> resetOffset(String brokerAddr, String topic, String group,
        long timestamp, boolean isForce, long timeoutMillis) {
        ResetOffsetRequestHeader header = new ResetOffsetRequestHeader();
        header.setTopic(topic);
        header.setGroup(group);
        header.setTimestamp(timestamp);
        header.setForce(isForce);
        return this.getAdmin().invokeBrokerToResetOffset(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<ConsumerConnection> getConsumerConnectionList(String brokerAddr, String group,
        long timeoutMillis) {
        GetConsumerConnectionListRequestHeader header = new GetConsumerConnectionListRequestHeader();
        header.setConsumerGroup(group);
        return this.getAdmin().getConsumerConnectionList(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<GroupList> queryTopicConsumeByWho(String brokerAddr, String topic, long timeoutMillis) {
        QueryTopicConsumeByWhoRequestHeader header = new QueryTopicConsumeByWhoRequestHeader();
        header.setTopic(topic);
        return this.getAdmin().queryTopicConsumeByWho(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<TopicList> queryTopicsByConsumer(String brokerAddr, String group, long timeoutMillis) {
        QueryTopicsByConsumerRequestHeader header = new QueryTopicsByConsumerRequestHeader();
        header.setGroup(group);
        return this.getAdmin().queryTopicsByConsumer(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<SubscriptionGroupConfig> getSubscriptionGroupConfig(String brokerAddr, String group,
        long timeoutMillis) {
        return supplyBlocking(() -> {
            try {
                return this.getClient().getSubscriptionGroupConfig(brokerAddr, group, timeoutMillis);
            } catch (Exception e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> updateSubscriptionGroupConfig(String brokerAddr, SubscriptionGroupConfig config,
        long timeoutMillis) {
        return this.getAdmin().updateOrCreateSubscriptionGroup(brokerAddr, config, timeoutMillis);
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionGroup(String brokerAddr, String group, boolean cleanOffset,
        long timeoutMillis) {
        DeleteSubscriptionGroupRequestHeader header = new DeleteSubscriptionGroupRequestHeader();
        header.setGroupName(group);
        header.setCleanOffset(cleanOffset);
        return this.getAdmin().deleteSubscriptionGroup(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<List<MessageExt>> queryMessage(String brokerAddr, String topic, String key, int maxNum,
        long beginTimestamp, long endTimestamp, boolean uniqueKey, boolean decompressBody, long timeoutMillis) {
        QueryMessageRequestHeader header = new QueryMessageRequestHeader();
        header.setTopic(topic);
        header.setKey(key);
        header.setMaxNum(maxNum);
        header.setBeginTimestamp(beginTimestamp);
        header.setEndTimestamp(endTimestamp);
        return this.getAdmin().queryMessage(brokerAddr, uniqueKey, decompressBody, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<MessageExt> viewMessage(String brokerAddr, String topic, long physicalOffset,
        long timeoutMillis) {
        ViewMessageRequestHeader header = new ViewMessageRequestHeader();
        header.setTopic(topic);
        header.setOffset(physicalOffset);
        return this.getAdmin().viewMessage(brokerAddr, header, timeoutMillis);
    }

    @Override
    public CompletableFuture<Long> searchOffsetByTimestamp(String brokerAddr, MessageQueue messageQueue,
        long timestamp, long timeoutMillis) {
        return supplyBlocking(() -> {
            try {
                return this.getClient().searchOffset(brokerAddr, messageQueue, timestamp, timeoutMillis);
            } catch (Exception e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
    }
}
