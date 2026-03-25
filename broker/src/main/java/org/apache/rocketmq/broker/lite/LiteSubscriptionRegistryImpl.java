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
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.NotifyUnsubscribeLiteRequestHeader;

public class LiteSubscriptionRegistryImpl extends ServiceThread implements LiteSubscriptionRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    protected final ConcurrentMap<String/*clientId*/, Channel> clientChannels = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String/*clientId*/, LiteSubscription> client2Subscription = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String/*lmqName*/, Set<ClientGroup>> liteTopic2Group = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String/*topic*/, Set<String/*group*/>> wildcardGroupMap = new ConcurrentHashMap<>();
    private final Cache<String/*group*/, List<ClientGroup>> wildcardClientCache =
        CacheBuilder.newBuilder().maximumSize(2000).expireAfterWrite(30, TimeUnit.SECONDS).build();

    protected final List<LiteCtlListener> listeners = new ArrayList<>();
    private final BrokerController brokerController;
    private final AbstractLiteLifecycleManager liteLifecycleManager;

    public LiteSubscriptionRegistryImpl(BrokerController brokerController,
        AbstractLiteLifecycleManager liteLifecycleManager) {
        this.brokerController = brokerController;
        this.liteLifecycleManager = liteLifecycleManager;
    }

    // Number of active liteTopic references.
    // [(client1, liteTopic1), (client2, liteTopic1)] counts as two active references.
    protected final AtomicInteger activeNum = new AtomicInteger(0);

    @Override
    public void updateClientChannel(String clientId, Channel channel) {
        clientChannels.put(clientId, channel);
    }

    @Override
    public void addPartialSubscription(String clientId, String group, String topic, Set<String> lmqNameSet,
        OffsetOption offsetOption) {
        long maxCount = brokerController.getBrokerConfig().getMaxLiteSubscriptionCount();
        if (getActiveSubscriptionNum() >= maxCount) {
            // No need to check existence, if reach here, it must be new.
            throw new LiteQuotaException("lite subscription quota exceeded " + maxCount);
        }
        if (LiteMetadataUtil.isWildcardGroup(group, brokerController)) {
            throw new IllegalStateException("subscribe lite operation is not supported for this group");
        }

        LiteSubscription thisSub = getOrCreateLiteSubscription(clientId, group, topic);
        // Utilize existing string object
        final ClientGroup clientGroup = new ClientGroup(clientId, thisSub.getGroup());
        for (String lmqName : lmqNameSet) {
            if (!liteLifecycleManager.isSubscriptionActive(topic, lmqName)) {
                continue;
            }
            thisSub.addLiteTopic(lmqName);
            // First remove the old subscription
            if (LiteMetadataUtil.isSubLiteExclusive(group, brokerController)) {
                excludeClientByLmqName(clientId, group, lmqName);
            }
            resetOffset(lmqName, group, clientId, offsetOption);
            addTopicGroup(clientGroup, lmqName);
        }
    }

    @Override
    public void removePartialSubscription(String clientId, String group, String topic, Set<String> lmqNameSet) {
        LiteSubscription thisSub = getOrCreateLiteSubscription(clientId, group, topic);
        ClientGroup clientGroup = new ClientGroup(clientId, thisSub.getGroup());
        boolean isResetOffsetOnUnsubscribe = LiteMetadataUtil.isResetOffsetOnUnsubscribe(group, brokerController);
        for (String lmqName : lmqNameSet) {
            thisSub.removeLiteTopic(lmqName);
            removeTopicGroup(clientGroup, lmqName, isResetOffsetOnUnsubscribe);
        }
    }

    @Override
    public void addCompleteSubscription(String clientId, String group, String topic, Set<String> lmqNameAll, long version) {
        Set<String> lmqNameNew;
        if (LiteMetadataUtil.isWildcardGroup(group, brokerController)) {
            lmqNameNew = Collections.singleton(mockLmqNameForWildcardGroup(topic, group));
            markWildcardGroup(topic, group);
        } else {
            lmqNameNew = lmqNameAll.stream()
                .filter(lmqName -> liteLifecycleManager.isSubscriptionActive(topic, lmqName))
                .collect(Collectors.toSet());
        }

        LiteSubscription thisSub = getOrCreateLiteSubscription(clientId, group, topic);
        Set<String> lmqNamePrev = thisSub.getLiteTopicSet();
        // Find topics to remove (in current set but not in new set)
        Set<String> lmqNameRemove = lmqNamePrev.stream()
            .filter(lmqName -> !lmqNameNew.contains(lmqName))
            .collect(Collectors.toSet());

        ClientGroup clientGroup = new ClientGroup(clientId, thisSub.getGroup());
        lmqNameRemove.forEach(lmqName -> {
            thisSub.removeLiteTopic(lmqName);
            removeTopicGroup(clientGroup, lmqName, false);
        });
        lmqNameNew.forEach(lmqName -> {
            thisSub.addLiteTopic(lmqName);
            addTopicGroup(clientGroup, lmqName);
        });
    }

    @Override
    public void removeCompleteSubscription(String clientId) {
        clientChannels.remove(clientId);
        LiteSubscription thisSub = client2Subscription.remove(clientId);
        if (thisSub == null) {
            return;
        }
        LOGGER.info("removeCompleteSubscription, topic:{}, group:{}, clientId:{}", thisSub.getTopic(), thisSub.getGroup(), clientId);
        ClientGroup clientGroup = new ClientGroup(clientId, thisSub.getGroup());
        thisSub.getLiteTopicSet().forEach(lmqName -> {
            removeTopicGroup(clientGroup, lmqName, false);
        });
        for (LiteCtlListener listener : listeners) {
            listener.onRemoveAll(clientId, thisSub.getGroup());
        }
    }

    @Override
    public void addListener(LiteCtlListener listener) {
        listeners.add(listener);
    }

    /**
     * Get all subscribers for a specific LMQ, with optional group filtering.
     * This method returns different types based on the subscription scenario:
     * 1. When there's only one subscriber, return List<ClientGroup>
     * 2. When group is specified, return List<ClientGroup> containing subscribers of that group
     * 3. When group is null and multiple groups exist, return Map<String, List<ClientGroup>>
     *    mapping each group to its subscribers
     */
    @Override
    public SubscriberWrapper getAllSubscriber(String group, String lmqName) {
        String topic = LiteUtil.getParentTopic(lmqName);

        if (group != null) {
            if (LiteMetadataUtil.isWildcardGroup(group, brokerController)) {
                return getWildcardSubscriber(group, topic);
            }
            SubscriberWrapper.ListWrapper wrapper = new SubscriberWrapper.ListWrapper();
            Set<ClientGroup> subscribers = liteTopic2Group.get(lmqName);
            if (subscribers != null) {
                wrapper.getClients().addAll(subscribers.stream()
                    .filter(clientGroup -> group.equals(clientGroup.group))
                    .collect(Collectors.toSet()));
            }
            return wrapper;
        } else {
            SubscriberWrapper.MapWrapper wrapper = new SubscriberWrapper.MapWrapper();
            Set<ClientGroup> subscribers = liteTopic2Group.get(lmqName);
            if (subscribers != null) {
                for (ClientGroup clientGroup : subscribers) {
                    wrapper.getGroupMap().computeIfAbsent(clientGroup.group, k -> new ArrayList<>()).add(clientGroup);
                }
            }
            Set<String> wildcardGroups = wildcardGroupMap.get(topic);
            if (wildcardGroups != null) {
                for (String wildcardGroup : wildcardGroups) {
                    List<ClientGroup> wildcardClients = getWildcardGroupClients(topic, wildcardGroup);
                    if (CollectionUtils.isNotEmpty(wildcardClients)) {
                        wrapper.getGroupMap().putIfAbsent(wildcardGroup, wildcardClients);
                    }
                }
            }
            return wrapper;
        }
    }

    @Override
    public SubscriberWrapper.ListWrapper getWildcardSubscriber(String group, String topic) {
        return new SubscriberWrapper.ListWrapper(getWildcardGroupClients(topic, group));
    }

    /**
     * Cleans up subscription for the given LMQ name.
     * Removes all related client subscriptions and notifies listeners.
     *
     * @param lmqName the LMQ name to clean up
     */
    @Override
    public void cleanSubscription(String lmqName, boolean notifyClient) {
        Set<ClientGroup> topicGroupSet = liteTopic2Group.remove(lmqName);
        if (CollectionUtils.isEmpty(topicGroupSet)) {
            return;
        }
        for (ClientGroup topicGroup : topicGroupSet) {
            LiteSubscription liteSubscription = client2Subscription.get(topicGroup.clientId);
            if (liteSubscription == null) {
                continue;
            }
            if (liteSubscription.removeLiteTopic(lmqName)) {
                if (notifyClient) {
                    notifyUnsubscribeLite(topicGroup.clientId, topicGroup.group, lmqName);
                }
                activeNum.decrementAndGet();
            }
        }
    }

    protected void addTopicGroup(ClientGroup clientGroup, String lmqName) {
        Set<ClientGroup> topicGroupSet = liteTopic2Group
            .computeIfAbsent(lmqName, k -> ConcurrentHashMap.newKeySet());
        if (topicGroupSet.add(clientGroup)) {
            activeNum.incrementAndGet();
            invalidateWildcardCacheIfNecessary(clientGroup.group);
            for (LiteCtlListener listener : listeners) {
                listener.onRegister(clientGroup.clientId, clientGroup.group, lmqName);
            }
        }
    }

    protected void removeTopicGroup(ClientGroup clientGroup, String lmqName, boolean resetOffset) {
        Set<ClientGroup> topicGroupSet = liteTopic2Group.get(lmqName);
        if (topicGroupSet == null) {
            return;
        }
        if (topicGroupSet.remove(clientGroup)) {
            activeNum.decrementAndGet();
            invalidateWildcardCacheIfNecessary(clientGroup.group);
            for (LiteCtlListener listener : listeners) {
                listener.onUnregister(clientGroup.clientId, clientGroup.group, lmqName);
            }
            if (resetOffset) {
                resetOffset(lmqName, clientGroup.group, clientGroup.clientId,
                    new OffsetOption(OffsetOption.Type.POLICY, OffsetOption.POLICY_MIN_VALUE));
            }
        }
        if (topicGroupSet.isEmpty()) {
            liteTopic2Group.remove(lmqName);
            unmarkWildcardGroupIfNecessary(lmqName);
        }
    }

    /**
     * Remove clients that subscribe to the same liteTopic under the same group
     */
    protected void excludeClientByLmqName(String newClientId, String group, String lmqName) {
        Set<ClientGroup> clientSet = liteTopic2Group.get(lmqName);
        if (CollectionUtils.isEmpty(clientSet)) {
            return;
        }
        List<ClientGroup> toRemove = clientSet.stream()
            .filter(clientGroup -> Objects.equals(group, clientGroup.group))
            .collect(Collectors.toList());

        toRemove.forEach(clientGroup -> {
            LiteSubscription liteSubscription = client2Subscription.get(clientGroup.clientId);
            if (liteSubscription != null) {
                liteSubscription.removeLiteTopic(lmqName);
                // remove client if no more liteTopic
                if (liteSubscription.getLiteTopicSet().isEmpty()) {
                    client2Subscription.remove(clientGroup.clientId);
                }
            }
            notifyUnsubscribeLite(clientGroup.clientId, clientGroup.group, lmqName);
            boolean resetOffset = LiteMetadataUtil.isResetOffsetInExclusiveMode(group, brokerController);
            LOGGER.info("excludeClientByLmqName group:{}, lmqName:{}, resetOffset:{}, clientId:{} -> {}",
                group, lmqName, resetOffset, clientGroup.clientId, newClientId);
            removeTopicGroup(clientGroup, lmqName, resetOffset);
        });
    }

    /**
     * Notify the client to remove the liteTopic subscription from its local memory
     */
    protected void notifyUnsubscribeLite(String clientId, String group, String lmqName) {
        String topic = LiteUtil.getParentTopic(lmqName);
        String liteTopic = LiteUtil.getLiteTopic(lmqName);
        Channel channel = clientChannels.get(clientId);
        if (channel == null) {
            LOGGER.warn("notifyUnsubscribeLite but channel is null, liteTopic:{}, group:{}, topic:{}, clientId:{},",
                liteTopic, group, topic, clientId);
            return;
        }

        NotifyUnsubscribeLiteRequestHeader header = new NotifyUnsubscribeLiteRequestHeader();
        header.setClientId(clientId);
        header.setConsumerGroup(group);
        header.setLiteTopic(liteTopic);
        brokerController.getBroker2Client().notifyUnsubscribeLite(channel, header);
        LOGGER.info("notifyUnsubscribeLite liteTopic:{}, group:{}, topic:{}, clientId:{}", liteTopic, group, topic, clientId);
    }

    @Override
    public LiteSubscription getLiteSubscription(String clientId) {
        return client2Subscription.get(clientId);
    }

    @Override
    public int getActiveSubscriptionNum() {
        return activeNum.get();
    }

    @Override
    public List<String> getAllClientIdByGroup(String group) {
        return client2Subscription.entrySet().stream()
            .filter(entry -> entry.getValue().getGroup().equals(group))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    protected void resetOffset(String lmqName, String group, String clientId, OffsetOption offsetOption) {
        if (null == offsetOption) {
            return;
        }
        Long targetOffset = null;
        long currentOffset = brokerController.getConsumerOffsetManager().queryOffset(group, lmqName, 0);
        switch (offsetOption.getType()) {
            case POLICY:
                if (OffsetOption.POLICY_MIN_VALUE == offsetOption.getValue()) {
                    targetOffset = 0L;
                } else if (OffsetOption.POLICY_MAX_VALUE == offsetOption.getValue()) {
                    targetOffset = liteLifecycleManager.getMaxOffsetInQueue(lmqName);
                }
                break;
            case OFFSET:
                targetOffset = offsetOption.getValue();
                break;
            case TAIL_N:
                if (currentOffset >= 0) { // only when consumer offset exists
                    targetOffset = Math.max(0L, currentOffset - offsetOption.getValue());
                }
                break;
            case TIMESTAMP:
                // timestamp option is disabled silently for now
                break;
        }

        LOGGER.info("try to reset lite offset. {}, {}, {}, {}, current:{}, target:{}",
            group, lmqName, clientId, offsetOption, currentOffset, targetOffset);
        if (targetOffset != null && currentOffset != targetOffset) {
            brokerController.getConsumerOffsetManager().assignResetOffset(lmqName, group, 0, targetOffset);
            brokerController.getPopLiteMessageProcessor().getConsumerOrderInfoManager().remove(lmqName, group);
        }
    }

    private LiteSubscription getOrCreateLiteSubscription(String clientId, String group, String topic) {
        LiteSubscription curLiteSubscription = ConcurrentHashMapUtils.computeIfAbsent(client2Subscription, clientId,
            k -> new LiteSubscription().setGroup(group).setTopic(topic));
        assert curLiteSubscription != null;
        return curLiteSubscription;
    }

    private void invalidateWildcardCacheIfNecessary(String group) {
        if (LiteMetadataUtil.isWildcardGroup(group, brokerController)) {
            wildcardClientCache.invalidate(group);
        }
    }

    private void markWildcardGroup(String topic, String group) {
        wildcardGroupMap.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(group);
    }

    private void unmarkWildcardGroupIfNecessary(String lmqName) {
        if (!LiteUtil.isLiteTopicQueue(lmqName)) { // must be topic@group
            String[] topicAtGroup = StringUtils.split(lmqName);
            if (null == topicAtGroup || topicAtGroup.length != 2) {
                return;
            }
            wildcardGroupMap.computeIfPresent(topicAtGroup[0], (k, v) -> {
                v.remove(topicAtGroup[1]);
                return v.isEmpty() ? null : v;
            });
        }
    }

    private String mockLmqNameForWildcardGroup(String topic, String group) {
        return topic + "@" + group;
    }

    private List<ClientGroup> getWildcardGroupClients(String topic, String group) {
        List<ClientGroup> list = null;
        try {
            list = wildcardClientCache.get(group, () -> {
                Set<ClientGroup> clientSet = liteTopic2Group.get(mockLmqNameForWildcardGroup(topic, group));
                return clientSet != null ? new ArrayList<>(clientSet) : Collections.emptyList();
            });
        } catch (ExecutionException ignored) {
        }
        return list;
    }

    @Override
    public void run() {
        LOGGER.info("Start checking lite subscription.");
        while (!this.isStopped()) {
            long checkInterval = brokerController.getBrokerConfig().getLiteSubscriptionCheckInterval();
            this.waitForRunning(checkInterval);

            long checkTimeout = brokerController.getBrokerConfig().getLiteSubscriptionCheckTimeoutMills();
            cleanupExpiredSubscriptions(checkTimeout);
        }
        LOGGER.info("End checking lite subscription.");
    }

    /**
     * Cleans up expired client subscriptions based on the provided timeout.
     *
     * @param checkTimeout the timeout in milliseconds to determine if a subscription is expired
     */
    @VisibleForTesting
    protected void cleanupExpiredSubscriptions(long checkTimeout) {
        // Step 1: Find expired clients and their subscription information
        long currentTime = System.currentTimeMillis();
        List<Map.Entry<String, LiteSubscription>> expiredEntries = client2Subscription.entrySet()
            .stream()
            .filter(entry -> currentTime - entry.getValue().getUpdateTime() > checkTimeout)
            .collect(Collectors.toList());

        // Step 2: Remove expired clients and their subscriptions
        expiredEntries.forEach(expiredEntry -> {
            String clientId = expiredEntry.getKey();
            LiteSubscription liteSubscription = expiredEntry.getValue();
            String group = liteSubscription.getGroup();
            String topic = liteSubscription.getTopic();
            removeCompleteSubscription(clientId);
            LOGGER.info("Remove expired LiteSubscription, topic: {}, group: {}, clientId: {}, timeout: {}ms, expired: {}ms",
                topic, group, clientId, checkTimeout, System.currentTimeMillis() - liteSubscription.getUpdateTime());
        });
    }

}