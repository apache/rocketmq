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
package org.apache.rocketmq.broker.topic;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/**
 * Broker-side cache of Topic route info, refreshed from the NameServer.
 *
 * <p>A broker is normally agnostic about topic routing
 *    ---- producers and consumers query the NameServer directly.
 * However, a few broker features need the broker itself to know
 * where a topic's queues live, so this class maintains a local,
 * periodically-refreshed view of that routing:
 *
 * <ul>
 *   <li>
 *       <b>Lite Topic sharding</b>
 *       {@code LiteShardingImpl} uses {@link #tryToFindTopicPublishInfo}
 *       to obtain the parent topic's write  queues
 *       and consistent-hash each LMQ onto the owning broker.
 *   </li>
 *   <li>
 *       <b>Message forwarding</b>
 *       when a broker relays a message to another broker, it looks up the target address via
 *       * {@link #findBrokerAddressInPublish}
 *       * {@link #findBrokerAddressInSubscribe}.
 *   </li>
 *   <li>
 *       <b>Pop assignment / escape bridge</b>
 *       {@link #topicSubscribeInfoTable} and {@link #topicRouteTable}
 *       feed the queue assignments used by Pop consumption.
 *   </li>
 * </ul>
 *
 * <p>Routing is cached in four concurrent maps
 *  1. publish info
 *  2. subscribe info,
 *  3. broker addresses
 *  4. full route data
 * A single scheduled executor refreshes the cached topics at a fixed interval;
 * {@code ReentrantLock} serializes concurrent updates per topic, and lookup methods lazily pull on cache miss.
 */
public class TopicRouteInfoManager {

    private static final long GET_TOPIC_ROUTE_TIMEOUT = 3000L;
    private static final long LOCK_TIMEOUT_MILLIS = 3000L;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final Lock lockNamesrv = new ReentrantLock();
    /**
     * Topic → {@link TopicRouteData}, the <b>raw NameServer response</b>(unprocessed).
     * This is the most complete cache and the single source of truth
     * from which the other three tables are derived.
     *
     * <pre>
     * Key: topic name (e.g. "TopicLMQParent")
     * Value: TopicRouteData {
     *     queueDatas:        [QueueData(brokerName, read/writeQueueNums, perm) ...]   // queue count per broker
     *     brokerDatas:       [BrokerData(cluster, brokerName, brokerAddrs) ...]       // broker master/slave addrs
     *     filterServerTable: Map&lt;brokerAddr, filterServerList&gt;                     // deprecated
     * }
     * </pre>
     *
     * <p><b>Source:</b> entirely from NameServer,
     * the untouched return value of {@code BrokerOuterAPI#getTopicRouteInfoFromNameServer}.
     * Used for {@code topicRouteDataChanged} version comparison and the escape-bridge queue assignment.
     */
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    /**
     * BrokerName → (brokerId → address), <b>parsed from TopicRouteData</b>.
     * Every route update splits out the {@code brokerDatas} and merges them in,
     * so this table <b>accumulates across topics</b> and can resolve addresses without any topic context.
     *
     * <pre>
     * Key: brokerName (e.g. "broker-a")
     * Value: {
     *     0L -> "10.0.0.1:10911"   // master (MASTER_ID = 0)
     *     1L -> "10.0.0.2:10911"   // slave
     *     ...
     * }
     * </pre>
     *
     * <p><b>Source:</b> parsed from {@link #topicRouteTable}'s {@code TopicRouteData.brokerDatas}.
     * Consumed by {@link #findBrokerAddressInPublish} (master only)
     * and {@link #findBrokerAddressInSubscribe} (by brokerId, with slave fallback).
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<>();
    /**
     * Topic → {@link TopicPublishInfo}, <b>parsed from TopicRouteData</b>,
     * keeping only the <b>writable-queue</b> view.
     * Direct data source for message sending and Lite-topic sharding.
     *
     * <pre>
     * Key: topic name
     * Value: TopicPublishInfo {
     *     messageQueueList: [MessageQueue(topic, brokerName, queueId) ...]  // writeQueueNums queues only
     *     sendWhichQueue:   ThreadLocalIndex                             // round-robin cursor
     * }
     * </pre>
     *
     * <p><b>Source:</b> parsed from {@link #topicRouteTable}
     * via {@code MQClientInstance#topicRouteData2TopicPublishInfo}.
     * Consumed by {@link #tryToFindTopicPublishInfo} (used by LiteShardingImpl sharding).
     */
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();

    /**
     * Topic → Set&lt;MessageQueue&gt;, <b>parsed from TopicRouteData</b>,
     * keeping only the <b>readable-queue</b> view.
     * Data source for Pop-consumer queue assignment.
     *
     * <pre>
     * Key: topic name
     * Value: {
     *     MessageQueue(topic, brokerName, 0),
     *     MessageQueue(topic, brokerName, 1),
     *     ... (readQueueNums entries total)
     * }
     * </pre>
     *
     * <p><b>Source:</b> parsed from {@link #topicRouteTable}
     * via {@code MQClientInstance#topicRouteData2TopicSubscribeInfo},
     * after clearing {@code topicQueueMappingByBroker} (to avoid static-topic mapping noise).
     * Consumed by {@link #getTopicSubscribeInfo} (Pop assignment).
     */
    private final ConcurrentHashMap<String, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduledExecutorService;
    private BrokerController brokerController;

    public TopicRouteInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        this.scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TopicRouteInfoManagerScheduledThread"));

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                updateTopicRouteInfoFromNameServer();
            } catch (Exception e) {
                log.error("ScheduledTask: failed to pull TopicRouteData from NameServer", e);
            }
        }, 1000, this.brokerController.getBrokerConfig().getLoadBalancePollNameServerInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * Refresh the route info of every cached topic from the NameServer.
     * This method will not pull routeInfo of topics not known by this server.
     *
     * <p>Iterates the union of the two cache key sets and, per topic,
     * asks the NameServer for fresh routing.
     * The update scope is driven by which cache a topic appears in:
     * topics in {@link #topicRouteTable} also refresh their publish info (escape bridge),
     * while topics in {@link #topicSubscribeInfoTable} also refresh their subscribe info (Pop assignment).
     * A topic present in both refreshes both.
     */
    private void updateTopicRouteInfoFromNameServer() {
        final Set<String> topicSetForPopAssignment = this.topicSubscribeInfoTable.keySet();
        final Set<String> topicSetForEscapeBridge = this.topicRouteTable.keySet();
        final Set<String> topicsAll = Sets.union(topicSetForPopAssignment, topicSetForEscapeBridge);

        for (String topic : topicsAll) {
            boolean isNeedUpdatePublishInfo = topicSetForEscapeBridge.contains(topic);
            boolean isNeedUpdateSubscribeInfo = topicSetForPopAssignment.contains(topic);
            updateTopicRouteInfoFromNameServer(topic, isNeedUpdatePublishInfo, isNeedUpdateSubscribeInfo);
        }
    }

    /**
     * Update a single topic's route info from the NameServer.
     *
     * <p>Serialized by {@link #lockNamesrv} (bounded wait of {@value #LOCK_TIMEOUT_MILLIS}ms)
     * so concurrent callers for the same topic do not hammer the NameServer.
     * Based on the two flags, refreshes either or both of the publish-info and subscribe-info caches
     * from the fetched {@link TopicRouteData}.
     *
     * <p>When the NameServer reports {@code TOPIC_NOT_EXIST} for a non-retry
     * topic, the topic is dropped from the caches via {@link #cleanNoneRouteTopic}.
     *
     * @param topic the topic whose route info is to be refreshed
     * @param isNeedUpdatePublishInfo if {@code true}, refresh the publish-info caches
     *        * {@link #topicRouteTable},
     *        * {@link #brokerAddrTable},
     *        * {@link #topicPublishInfoTable}.
     * @param isNeedUpdateSubscribeInfo if {@code true}, refresh the subscribe-info cache
     *        ({@link #topicSubscribeInfoTable}) from the fetched route data
     */
    public void updateTopicRouteInfoFromNameServer(String topic, boolean isNeedUpdatePublishInfo,
        boolean isNeedUpdateSubscribeInfo) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    final TopicRouteData topicRouteData = this.brokerController.getBrokerOuterAPI()
                        .getTopicRouteInfoFromNameServer(topic, GET_TOPIC_ROUTE_TIMEOUT);
                    if (null == topicRouteData) {
                        log.warn("TopicRouteInfoManager: updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}.", topic);
                        return;
                    }

                    if (isNeedUpdateSubscribeInfo) {
                        this.updateSubscribeInfoTable(topicRouteData, topic);
                    }

                    if (isNeedUpdatePublishInfo) {
                        this.updateTopicRouteTable(topic, topicRouteData);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                } catch (MQBrokerException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    if (!NamespaceUtil.isRetryTopic(topic)
                        && ResponseCode.TOPIC_NOT_EXIST == e.getResponseCode()) {
                        // clean no used topic
                        cleanNoneRouteTopic(topic);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }
    }

    private boolean updateTopicRouteTable(String topic, TopicRouteData topicRouteData) {
        TopicRouteData old = this.topicRouteTable.get(topic);
        boolean changed = topicRouteData.topicRouteDataChanged(old);
        if (!changed) {
            if (!this.isNeedUpdateTopicRouteInfo(topic)) {
                return false;
            }
        } else {
            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
        }

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
        }

        TopicPublishInfo publishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
        publishInfo.setHaveTopicRouterInfo(true);
        this.updateTopicPublishInfo(topic, publishInfo);

        TopicRouteData cloneTopicRouteData = new TopicRouteData(topicRouteData);
        log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
        this.topicRouteTable.put(topic, cloneTopicRouteData);

        return true;
    }

    private boolean updateSubscribeInfoTable(TopicRouteData topicRouteData, String topic) {
        final TopicRouteData tmp = new TopicRouteData(topicRouteData);
        tmp.setTopicQueueMappingByBroker(null);
        Set<MessageQueue> newSubscribeInfo = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, tmp);
        Set<MessageQueue> oldSubscribeInfo = topicSubscribeInfoTable.get(topic);

        if (Objects.equals(newSubscribeInfo, oldSubscribeInfo)) {
            return false;
        }

        log.info("the topic[{}] subscribe message queue changed, old[{}] ,new[{}]", topic, oldSubscribeInfo, newSubscribeInfo);
        topicSubscribeInfoTable.put(topic, newSubscribeInfo);
        return true;

    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        final TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);
        return null == prev || !prev.ok();
    }

    private void cleanNoneRouteTopic(String topic) {
        // clean no used topic
        topicSubscribeInfoTable.remove(topic);
    }

    private void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev);
            }
        }
    }

    public void shutdown() {
        if (null != this.scheduledExecutorService) {
            this.scheduledExecutorService.shutdown();
        }
    }

    /**
     * Look up the publish info (writable queues) for a topic, pulling from the
     * NameServer on cache miss.
     *
     * <p>If the cached {@link TopicPublishInfo} is absent or stale
     * ({@code !ok()}), a synchronous refresh is triggered and the cache is
     * re-read. Used by Lite-topic sharding to obtain the parent topic's write
     * queues.
     *
     * @param topic the topic whose publish info is needed
     * @return the publish info, or {@code null} if the topic still has no route
     */
    public TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            this.updateTopicRouteInfoFromNameServer(topic, true, false);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        return topicPublishInfo;
    }

    /**
     * Resolve a broker's <b>master</b> address (brokerId ==
     * {@link MixAll#MASTER_ID}) from {@link #brokerAddrTable}.
     *
     * <p>Used when sending a message to another broker's master.
     *
     * @param brokerName the broker whose master address is needed
     * @return the master address, or {@code null} if the broker is unknown
     */
    public String findBrokerAddressInPublish(String brokerName) {
        if (brokerName == null) {
            return null;
        }
        Map<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    /**
     * Resolve a broker address by brokerId for consuming, with fallback.
     *
     * <p>Lookup order in {@link #brokerAddrTable}:
     * <ol>
     *   <li>exact {@code brokerId} match;</li>
     *   <li>if {@code brokerId} is a slave and not found, try
     *       {@code brokerId + 1};</li>
     *   <li>if still not found and {@code onlyThisBroker} is {@code false},
     *       fall back to an arbitrary address in the map.</li>
     * </ol>
     *
     * @param brokerName the broker whose address is needed
     * @param brokerId the preferred brokerId (0 = master, &gt;0 = slave)
     * @param onlyThisBroker if {@code true}, do not fall back to another
     *     broker's address when the exact match is missing
     * @return the resolved address, or {@code null} if none is available
     */
    public String findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        if (brokerName == null) {
            return null;
        }
        String brokerAddr = null;
        boolean found = false;

        Map<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            boolean slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && slave) {
                brokerAddr = map.get(brokerId + 1);
                found = brokerAddr != null;
            }

            if (!found && !onlyThisBroker) {
                Map.Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                found = true;
            }
        }

        return brokerAddr;

    }

    /**
     * Look up the readable queues for a topic, pulling from the NameServer on
     * cache miss.
     *
     * <p>If the cached {@link Set} of {@link MessageQueue} is absent or empty, a
     * synchronous refresh is triggered and the cache is re-read. Used by Pop
     * consumption for queue assignment.
     *
     * @param topic the topic whose subscribe info is needed
     * @return the set of readable queues, or {@code null} if the topic still
     *     has no route
     */
    public Set<MessageQueue> getTopicSubscribeInfo(String topic) {
        Set<MessageQueue> queues = topicSubscribeInfoTable.get(topic);
        if (null == queues || queues.isEmpty()) {
            this.updateTopicRouteInfoFromNameServer(topic, false, true);
            queues = this.topicSubscribeInfoTable.get(topic);
        }
        return queues;
    }
}
