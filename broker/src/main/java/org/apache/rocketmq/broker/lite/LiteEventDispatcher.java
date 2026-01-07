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

package org.apache.rocketmq.broker.lite;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class LiteEventDispatcher extends ServiceThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);
    private static final Object PRESENT = new Object();
    private static final long CLIENT_INACTIVE_INTERVAL = 10 * 1000; // inactive time when it has unprocessed events
    private static final long CLIENT_LONG_POLLING_INTERVAL = 30 * 1000 + 5000; // at least a period of long polling as 30s
    private static final long ACTIVE_CONSUMING_WINDOW = 5000;
    private static final double LOW_WATER_MARK = 0.2;
    private static final int BLACKLIST_EXPIRE_SECONDS = 10;
    private static final int SCAN_LOG_INTERVAL = 10000;

    private final BrokerController brokerController;
    private final LiteSubscriptionRegistry liteSubscriptionRegistry;
    private final AbstractLiteLifecycleManager liteLifecycleManager;
    private final ConsumerOffsetManager consumerOffsetManager;
    private ConsumerOrderInfoManager consumerOrderInfoManager;

    private final ConcurrentMap<String, ClientEventSet> clientEventMap = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<FullDispatchRequest> fullDispatchSet = new ConcurrentSkipListSet<>(COMPARATOR);
    private final ConcurrentMap<String, Object> fullDispatchMap = new ConcurrentHashMap<>(); // deduplication
    private final Cache<String, Object> blacklist =
        CacheBuilder.newBuilder().expireAfterWrite(BLACKLIST_EXPIRE_SECONDS, TimeUnit.SECONDS).build();
    private final Random random = ThreadLocalRandom.current();
    private long lastLogTime = System.currentTimeMillis();

    public LiteEventDispatcher(BrokerController brokerController,
        LiteSubscriptionRegistry liteSubscriptionRegistry, AbstractLiteLifecycleManager liteLifecycleManager) {
        this.brokerController = brokerController;
        this.liteSubscriptionRegistry = liteSubscriptionRegistry;
        this.liteLifecycleManager = liteLifecycleManager;
        this.consumerOffsetManager = brokerController.getConsumerOffsetManager();
    }

    public void init() {
        this.consumerOrderInfoManager = brokerController.getPopLiteMessageProcessor().getConsumerOrderInfoManager();
        this.liteSubscriptionRegistry.addListener(new LiteCtlListenerImpl());
    }

    /**
     * If event mode is enabled, try to dispatch event to one client when message arriving or available.
     * In most cases, there is only one subscriber for a LMQ under a consumer group,
     * but also supports multiple clients consuming in share mode.
     * When group is null, dispatch to all subscribers regardless of their group,
     * when group is specified, only dispatch to subscribers belonging to this group.
     * <p>
     * If the expected number of subscriptions by each client is small, disabling event mode can be a choice.
     */
    public void dispatch(String group, String lmqName, int queueId, long offset, long msgStoreTime) {
        if (!this.brokerController.getBrokerConfig().isEnableLiteEventMode()) {
            return;
        }
        if (queueId != 0 || !LiteUtil.isLiteTopicQueue(lmqName)) {
            return;
        }
        doDispatch(group, lmqName, null);
    }

    @SuppressWarnings("unchecked")
    private void doDispatch(String group, String lmqName, String excludeClientId) {
        if (!this.brokerController.getBrokerConfig().isEnableLiteEventMode()) {
            return;
        }
        Object subscribers = getAllSubscriber(group, lmqName);
        if (null == subscribers) {
            return;
        }
        if (subscribers instanceof List) {
            selectAndDispatch(lmqName, (List<ClientGroup>) subscribers, excludeClientId);
        }
        if (subscribers instanceof Map) {
            Map<String, List<ClientGroup>> map = (Map<String, List<ClientGroup>>) subscribers;
            map.forEach((key, value) -> selectAndDispatch(lmqName, value, excludeClientId));
        }
    }

    /**
     * Select an appropriate client from the client list and try to dispatch the event to it.
     * If there's only one client, dispatch directly to it.
     * If there are multiple clients, randomly select one and consider fallback options
     * Try to avoid dispatching to the excluded one but fallback if no other choice.
     *
     * @param clients all clients of one group
     * @param excludeClientId the client ID to exclude from selection, probably consuming blocked.
     */
    @VisibleForTesting
    public void selectAndDispatch(String lmqName, List<ClientGroup> clients, String excludeClientId) {
        if (!this.brokerController.getBrokerConfig().isEnableLiteEventMode()) {
            return;
        }
        if (CollectionUtils.isEmpty(clients)) {
            return;
        }

        String clientId = null; // the selected one
        if (clients.size() == 1) {
            clientId = clients.get(0).clientId;
            if (brokerController.getBrokerConfig().isEnableLitePopLog() && clientId.equals(excludeClientId)) {
                LOGGER.info("no others, still dispatch to {}, {}", clientId, lmqName);
            }
            if (!tryDispatchToClient(lmqName, clientId, clients.get(0).group)) {
                clientId = null;
            }
        } else {
            int start = random.nextInt(clients.size());
            boolean dispatched = false;
            List<ClientGroup> fallbackList = new ArrayList<>(clients.size());
            for (int i = 0; i < clients.size(); i++) {
                int index = (start + i) % clients.size();
                clientId = clients.get(index).clientId;
                if (clientId.equals(excludeClientId)) {
                    fallbackList.add(clients.get(index));
                    continue;
                }
                if (blacklist.getIfPresent(clientId) != null) {
                    fallbackList.add(clients.get(index));
                    continue;
                }
                if (tryDispatchToClient(lmqName, clientId, clients.get(index).group)) {
                    dispatched = true;
                    break;
                }
            }
            if (!dispatched) {
                clientId = null;
                for (ClientGroup clientGroup : fallbackList) {
                    if (tryDispatchToClient(lmqName, clientGroup.clientId, clientGroup.group)) {
                        clientId = clientGroup.clientId;
                        break;
                    }
                }
            }
        }
        if (clientId != null) {
            this.brokerController.getPopLiteMessageProcessor().getPopLiteLongPollingService()
                .notifyMessageArriving(clientId, true, 0, clients.get(0).group);
        }
    }

    /**
     * Try to dispatch an event to a selected client by adding it to the client's event queue.
     * If the event queue is full, mark a full dispatch for retry later.
     */
    @VisibleForTesting
    public boolean tryDispatchToClient(String lmqName, String clientId, String group) {
        ClientEventSet eventSet = clientEventMap.computeIfAbsent(clientId, key -> new ClientEventSet(group));
        if (eventSet.offer(lmqName)) {
            return true;
        }
        scheduleFullDispatch(clientId, group, blacklist.getIfPresent(clientId) != null);
        LOGGER.warn("client event set is full. {}", clientId);
        return false;
    }

    /**
     * Get an iterator for iterating over events for a specific client.
     * In lite event mode, returns events from the client's event queue,
     * or else returns topics from the client's subscription.
     */
    public Iterator<String> getEventIterator(String clientId) {
        if (this.brokerController.getBrokerConfig().isEnableLiteEventMode()) {
            return new EventSetIterator(clientEventMap.get(clientId));
        } else {
            LiteSubscription liteSubscription = liteSubscriptionRegistry.getLiteSubscription(clientId);
            return liteSubscription != null && liteSubscription.getLiteTopicSet() != null ?
                new LiteSubscriptionIterator(liteSubscription.getTopic(), liteSubscription.getLiteTopicSet().iterator())
                    : Collections.emptyIterator();
        }
    }

    /**
     * Perform a full dispatch for a client which was previously marked for a delayed full dispatch.
     * This always happens when a client's event queue is full or re-dispatching is needed.
     * It iterates through all LMQ topics subscribed by the client and dispatches events for those
     * with available messages.
     */
    public void doFullDispatch(String clientId, String group) {
        if (!this.brokerController.getBrokerConfig().isEnableLiteEventMode()) {
            return;
        }
        LiteSubscription subscription = liteSubscriptionRegistry.getLiteSubscription(clientId);
        if (null == subscription || CollectionUtils.isEmpty(subscription.getLiteTopicSet())) {
            LOGGER.info("client full dispatch, but no subscription. {}", clientId);
            return;
        }
        ClientEventSet eventSet = clientEventMap.computeIfAbsent(clientId, key -> new ClientEventSet(group));
        if (eventSet.maybeBlock()) {
            LOGGER.warn("client may block for a while, wait another period. {}", clientId);
            scheduleFullDispatch(clientId, group, true);
            return;
        }
        boolean isActiveConsuming = eventSet.isActiveConsuming();
        if (!eventSet.isLowWaterMark()) {
            LOGGER.warn("client event set high water mark, wait another period. {}, {}", clientId, isActiveConsuming);
            scheduleFullDispatch(clientId, group, !isActiveConsuming);
            return;
        }
        LOGGER.info("client full dispatch, {}, total:{}", clientId, subscription.getLiteTopicSet().size());
        int count = 0;
        for (String lmqName : subscription.getLiteTopicSet()) {
            long maxOffset = liteLifecycleManager.getMaxOffsetInQueue(lmqName);
            if (maxOffset <= 0) {
                continue;
            }
            long consumerOffset = consumerOffsetManager.queryOffset(group, lmqName, 0);
            if (consumerOffset >= maxOffset) {
                continue;
            }
            if (eventSet.offer(lmqName)) {
                if (count++ % 10 == 0) {
                    brokerController.getPopLiteMessageProcessor().getPopLiteLongPollingService()
                        .notifyMessageArriving(clientId, true, 0, group);
                }
            } else {
                LOGGER.warn("client event set full again, wait another period. {}, {}", clientId, isActiveConsuming);
                scheduleFullDispatch(clientId, group, !isActiveConsuming);
                break;
            }
        }
        brokerController.getPopLiteMessageProcessor().getPopLiteLongPollingService()
            .notifyMessageArriving(clientId, true, 0, group);
        LOGGER.info("client full dispatch finish. {}, dispatch:{}", clientId, count);
    }

    /**
     * Perform a full dispatch for all clients under a specific group, only invoked by admin for now.
     */
    public void doFullDispatchByGroup(String group) {
        List<String> clientIds = liteSubscriptionRegistry.getAllClientIdByGroup(group);
        LOGGER.info("do full dispatch by group, {}, size:{}", group, clientIds.size());
        for (String clientId : clientIds) {
            doFullDispatch(clientId, group);
        }
    }

    public void scheduleFullDispatch(String clientId, String group, boolean reentry) {
        if (fullDispatchMap.putIfAbsent(clientId, PRESENT) != null) {
            return;
        }
        int randomDelay = reentry ? random.nextInt(25 * 1000) : 0;
        fullDispatchSet.add(new FullDispatchRequest(clientId, group,
            brokerController.getBrokerConfig().getLiteEventFullDispatchDelayTime() + randomDelay));
    }

    /**
     * Get all subscribers for a specific LMQ, with optional group filtering.
     * To avoid unnecessary comparisons and wrapping, Object is used as the return type here.
     * This method returns different types based on the subscription scenario:
     * 1. When there's only one subscriber, return List<ClientGroup>
     * 2. When group is specified, return List<ClientGroup> containing subscribers of that group
     * 3. When group is null and multiple groups exist, return Map<String, List<ClientGroup>>
     *    mapping each group to its subscribers
     *
     * @return Object that can be either List<ClientGroup> or Map<String, List<ClientGroup>> or null if not found
     */
    @VisibleForTesting
    public Object getAllSubscriber(String group, String lmqName) {
        Set<ClientGroup> observers = liteSubscriptionRegistry.getSubscriber(lmqName);
        if (null == observers || observers.isEmpty()) {
            return null;
        }
        if (observers.size() == 1) {
            if (null == group || group.equals(observers.iterator().next().group)) {
                return new ArrayList<>(observers);
            }
            return null;
        }
        if (group != null) {
            List<ClientGroup> result = new ArrayList<>(4);
            for (ClientGroup ele : observers) {
                if (group.equals(ele.group)) {
                    result.add(ele);
                }
            }
            return !result.isEmpty() ? result : null;
        }

        Map<String, List<ClientGroup>> group2Clients = new HashMap<>(4);
        for (ClientGroup ele : observers) {
            group2Clients.computeIfAbsent(ele.group, k -> new ArrayList<>(2)).add(ele);
        }
        return group2Clients;
    }

    /**
     * Get the last access time of a client's event set.
     *
     * @param clientId the client id
     * @return the last access time in milliseconds, or -1 if client not found
     */
    public long getClientLastAccessTime(String clientId) {
        ClientEventSet eventSet = clientEventMap.get(clientId);
        if (eventSet != null) {
            return eventSet.lastAccessTime;
        }
        return -1;
    }

    @Override
    public String getServiceName() {
        if (brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + LiteEventDispatcher.class.getSimpleName();
        }
        return LiteEventDispatcher.class.getSimpleName();
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            long checkInterval = brokerController.getBrokerConfig().getLiteEventCheckInterval();
            this.waitForRunning(checkInterval);
            try {
                scan();
            } catch (Exception e) {
                LOGGER.error("LiteEventDispatcher-scan error.", e);
            }
        }
    }

    /**
     * Due to the event pre-allocation mechanism, it is necessary to perform
     * two main tasks to check inactive event queues and do full dispatch to reduce potential delivery latency.
     * 1. Check client event set for inactive clients and re-dispatches their events
     * 2. Process delayed full dispatch requests that are ready to be executed
     */
    public void scan() {
        boolean needLog = System.currentTimeMillis() - lastLogTime > SCAN_LOG_INTERVAL;

        // 1. check all client event set
        if (needLog) {
            LOGGER.info("Check client event set. size:{}", clientEventMap.size());
            lastLogTime = System.currentTimeMillis();
        }
        Iterator<Map.Entry<String, ClientEventSet>> iterator = clientEventMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ClientEventSet> entry = iterator.next();
            ClientEventSet eventSet = entry.getValue();
            if (!eventSet.maybeBlock()) {
                continue;
            }
            String clientId = entry.getKey();
            LOGGER.warn("remove inactive client and re-dispatch. {}, {}", clientId, eventSet.events.size());
            iterator.remove();
            blacklist.put(clientId, PRESENT);
            String event;
            while ((event = eventSet.poll()) != null) {
                doDispatch(eventSet.group, event, clientId); // may still dispatch to current client
            }
        }

        // 2. perform full dispatch
        if (needLog) {
            LOGGER.info("Begin to trigger full dispatch. size:{}, mapSize:{}", fullDispatchSet.size(), fullDispatchMap.size());
            lastLogTime = System.currentTimeMillis();
        }
        FullDispatchRequest request;
        while ((request = fullDispatchSet.pollFirst()) != null) {
            if (request.timestamp > System.currentTimeMillis()) {
                fullDispatchSet.add(request);
                break;
            }
            fullDispatchMap.remove(request.clientId);
            doFullDispatch(request.clientId, request.group);
        }
    }

    public int getEventMapSize() {
        return clientEventMap.size();
    }

    /**
     * We use dual data structure to maintain the event queue for each client
     * and ensure event deduplication to avoid duplicate events, although it
     * has a bit more memory usage than a single concurrent set.
     */
    class ClientEventSet {
        private final BlockingQueue<String> events;
        private final ConcurrentMap<String, Object> map = new ConcurrentHashMap<>();
        private final String group;
        private volatile long lastAccessTime = System.currentTimeMillis();
        private volatile long lastConsumeTime = System.currentTimeMillis();

        public ClientEventSet(String group) {
            this.group = group;
            events = new LinkedBlockingQueue<>(LiteMetadataUtil.getMaxClientEventCount(group, brokerController));
        }

        // return false if and only if the queue is full, has race condition with poll(), but no side effect.
        public boolean offer(String event) {
            if (events.remainingCapacity() == 0) {
                return false;
            }
            boolean rst;
            if (map.putIfAbsent(event, PRESENT) == null) {
                rst = events.offer(event);
                if (!rst) {
                    map.remove(event);
                }
            } else {
                rst = true;
            }
            return rst;
        }

        public String poll() {
            lastAccessTime = System.currentTimeMillis();
            String event = events.poll();
            if (event != null) {
                map.remove(event);
                lastConsumeTime = System.currentTimeMillis();
            }
            return event;
        }

        public boolean maybeBlock() {
            long inactiveTime = System.currentTimeMillis() - lastAccessTime;
            return inactiveTime > CLIENT_LONG_POLLING_INTERVAL
                || !events.isEmpty() && inactiveTime > CLIENT_INACTIVE_INTERVAL;
        }

        public boolean isLowWaterMark() {
            int used = events.size();
            return (double) used / (used + events.remainingCapacity()) < LOW_WATER_MARK;
        }

        public boolean isActiveConsuming() {
            return System.currentTimeMillis() - lastAccessTime < ACTIVE_CONSUMING_WINDOW;
        }

        public int size() {
            return events.size();
        }
    }

    class LiteCtlListenerImpl implements LiteCtlListener {

        @Override
        public void onRegister(String clientId, String group, String lmqName) {
            if (liteLifecycleManager.isLmqExist(lmqName)) {
                doDispatch(group, lmqName, null);
            }
        }

        @Override
        public void onUnregister(String clientId, String group, String lmqName) {
        }

        /**
         *  Mostly triggered when client channel closed, ensure that lite subscriptions is cleared before.
         */
        @Override
        public void onRemoveAll(String clientId, String group) {
            ClientEventSet eventSet = clientEventMap.remove(clientId);
            if (null == eventSet) {
                return;
            }
            LOGGER.warn("Maybe client offline. {}", clientId);
            String event;
            while ((event = eventSet.poll()) != null) {
                doDispatch(eventSet.group, event, clientId);
            }
        }
    }

    static class EventSetIterator implements Iterator<String> {
        private final ClientEventSet eventSet;

        public EventSetIterator(ClientEventSet eventSet) {
            this.eventSet = eventSet;
        }

        @Override
        public boolean hasNext() {
            return eventSet != null && !eventSet.events.isEmpty();
        }

        @Override
        public String next() {
            return eventSet.poll();
        }
    }

    static class LiteSubscriptionIterator implements Iterator<String> {
        private final Iterator<String> iterator;
        private final String parentTopic;
        public LiteSubscriptionIterator(String parentTopic, Iterator<String> iterator) {
            this.parentTopic = parentTopic;
            this.iterator = iterator;
        }
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public String next() {
            return iterator.next();
        }
    }

    static class FullDispatchRequest {
        private final String clientId;
        private final String group;
        private final long timestamp;
        public FullDispatchRequest(String clientId, String group, long delayMillis) {
            this.clientId = clientId;
            this.group = group;
            this.timestamp = System.currentTimeMillis() + delayMillis;
        }
    }

    // no need to compare group
    static final Comparator<FullDispatchRequest> COMPARATOR = (r1, r2) -> {
        if (null == r1 || null == r2 || null == r1.clientId || null == r2.clientId) {
            return 0;
        }
        if (r1.clientId.equals(r2.clientId)) {
            return 0;
        }
        int ret = Long.compare(r1.timestamp, r2.timestamp);
        if (ret != 0) {
            return ret;
        }
        return r1.clientId.compareTo(r2.clientId);
    };
}
