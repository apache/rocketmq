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
import java.util.HashSet;
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
import org.apache.rocketmq.common.lite.LitePatternMatcher;
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

    private final ExclusiveEvictionTombstones exclusiveEvictionTombstones = new ExclusiveEvictionTombstones();

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
                // Boundary case: this client may have a stale tombstone from a previous eviction.
                // Since it is now actively re-claiming the lmqName, clear its own tombstone so
                // subsequent popLiteTopic is not blocked by the stale mark.
                exclusiveEvictionTombstones.remove(clientId, lmqName);
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
        addCompleteSubscription(clientId, group, topic, lmqNameAll, Collections.emptySet(), version);
    }

    @Override
    public void addCompleteSubscription(String clientId, String group, String topic, Set<String> lmqNameAll,
        Set<String> wildcardPatterns, long version) {
        Set<String> lmqNameNew;
        boolean isWildcardGroup = LiteMetadataUtil.isWildcardGroup(group, brokerController);
        boolean isPatternMode = isWildcardGroup && wildcardPatterns != null && !wildcardPatterns.isEmpty();

        // Compute the target lmqName set BEFORE any mutation. expandWildcardPatterns is read-only
        // (it scans existing lite-topics via the lifecycle manager and matches patterns), so it is
        // safe to call pre-flight. This lets us project the net reference delta and enforce the
        // broker-wide quota BEFORE touching subscription state — a quota failure then leaves no
        // patterns / wildcard-group marks behind that later re-expansion would mistake for a live
        // (deferred-effect) subscription.
        if (isPatternMode) {
            lmqNameNew = expandWildcardPatterns(topic, wildcardPatterns);
        } else if (isWildcardGroup) {
            lmqNameNew = Collections.singleton(mockLmqNameForWildcardGroup(topic, group));
        } else {
            lmqNameNew = lmqNameAll.stream()
                .filter(lmqName -> liteLifecycleManager.isSubscriptionActive(topic, lmqName))
                .collect(Collectors.toSet());
        }

        LiteSubscription thisSub = getOrCreateLiteSubscription(clientId, group, topic);
        Set<String> lmqNamePrev = thisSub.getLiteTopicSet();

        // Pre-flight quota check: project the NET change in (client, lmqName) references this
        // operation would register — lmqNames added that are not already held, MINUS lmqNames held
        // that this operation will remove. Crediting removals means a set-replacing client at the
        // limit whose net delta is zero (or negative) is not falsely rejected. A re-sync of an
        // identical set has wouldAdd == 0 and passes trivially. The check runs before any mutation
        // (setWildcardPatterns / markWildcardGroup / add-remove below), so a throw leaves only the
        // harmless empty placeholder LiteSubscription created by getOrCreateLiteSubscription — no
        // patterns, no wildcard-group mark, no active references. The throw maps to
        // LITE_SUBSCRIPTION_QUOTA_EXCEEDED via the processor's existing catch.
        int wouldAdd = Math.max(0,
            (int) lmqNameNew.stream().filter(lmqName -> !lmqNamePrev.contains(lmqName)).count()
            - (int) lmqNamePrev.stream().filter(lmqName -> !lmqNameNew.contains(lmqName)).count());
        checkQuotaOrThrow(wouldAdd);

        // Quota passed — now apply the authoritative intent and group marks.
        if (isPatternMode) {
            // Persist the patterns (authoritative intent) and register the eagerly-expanded
            // lmqNames as a normal subscription; the topic@group synthetic key is NOT used for
            // pattern-mode groups.
            thisSub.setWildcardPatterns(wildcardPatterns);
            markWildcardGroup(topic, group);
        } else if (isWildcardGroup) {
            // Legacy wildcard group: receive all lite-topics under the parent topic via the
            // synthetic topic@group key. Clear any previously stored patterns so a client that
            // transitions from pattern mode back to legacy mode is no longer classified as
            // pattern-mode (doFullDispatchForWildcardGroup / reexpandWildcardPatterns key off a
            // non-empty wildcardPatterns set).
            thisSub.setWildcardPatterns(Collections.emptySet());
            markWildcardGroup(topic, group);
        } else {
            // Non-wildcard group: a normal subscription. Clear stale patterns from any prior
            // wildcard incarnation so the group is not misclassified downstream.
            thisSub.setWildcardPatterns(Collections.emptySet());
        }

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
        // Tombstone operations only apply to exclusive groups.
        if (LiteMetadataUtil.isSubLiteExclusive(group, brokerController)) {
            // Boundary case: if any lmqName in the client's reported full subscription still has
            // a tombstone, the previous notifyUnsubscribeLite was likely lost. Re-send the
            // unsubscribe notification to drive the client's local state to converge.
            lmqNameNew.stream()
                .filter(lmqName -> exclusiveEvictionTombstones.contains(clientId, lmqName))
                .forEach(lmqName -> {
                    LOGGER.info("re-notify unsubscribe for tombstoned lmqName, clientId:{}, group:{}, lmqName:{}",
                        clientId, group, lmqName);
                    notifyUnsubscribeLite(clientId, group, lmqName);
                });
            // Clean exclusive-eviction tombstones for liteTopics no longer in the client's full subscription set
            exclusiveEvictionTombstones.removeStale(clientId, lmqNameNew);
        }
    }

    /**
     * Pre-flight quota check for the request/response registration paths. Throws
     * {@link LiteQuotaException} (mapped to {@code LITE_SUBSCRIPTION_QUOTA_EXCEEDED} by the
     * processor) if the projected active count would exceed the configured broker-wide limit.
     *
     * @param wouldAdd the NET number of (client, lmqName) references this operation would add
     *                (new additions minus pending removals, floored at 0); 0 for a pure
     *                "is the broker already at the limit" check
     */
    private void checkQuotaOrThrow(int wouldAdd) {
        long maxCount = brokerController.getBrokerConfig().getMaxLiteSubscriptionCount();
        if ((long) getActiveSubscriptionNum() + wouldAdd > maxCount) {
            throw new LiteQuotaException("lite subscription quota exceeded " + maxCount
                + ", current: " + getActiveSubscriptionNum() + ", would add: " + wouldAdd);
        }
    }

    /**
     * Eagerly expand wildcard patterns against the existing lite-topics under {@code parentTopic}.
     * Returns the set of matched <em>full lmqNames</em> that this broker is responsible for
     * (sharding-aware via {@link AbstractLiteLifecycleManager#isSubscriptionActive}).
     */
    private Set<String> expandWildcardPatterns(String parentTopic, Set<String> wildcardPatterns) {
        List<String> candidates = liteLifecycleManager.collectByParentTopic(parentTopic);
        if (candidates.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> matchedLmqNames = new HashSet<>();
        for (String lmqName : candidates) {
            String child = LiteUtil.getLiteTopic(lmqName);
            if (child == null) {
                continue;
            }
            if (LitePatternMatcher.matchesAny(wildcardPatterns, child)
                && liteLifecycleManager.isSubscriptionActive(parentTopic, lmqName)) {
                matchedLmqNames.add(lmqName);
            }
        }
        return matchedLmqNames;
    }

    @Override
    public int reexpandWildcardPatterns(String clientId) {
        LiteSubscription thisSub = client2Subscription.get(clientId);
        if (thisSub == null || CollectionUtils.isEmpty(thisSub.getWildcardPatterns())) {
            return 0;
        }
        String topic = thisSub.getTopic();
        String group = thisSub.getGroup();
        Set<String> matched = expandWildcardPatterns(topic, thisSub.getWildcardPatterns());
        Set<String> current = thisSub.getLiteTopicSet();
        // Register only the newly-matched lmqNames; existing ones are already subscribed.
        Set<String> toAdd = matched.stream().filter(lmqName -> !current.contains(lmqName)).collect(Collectors.toSet());
        if (toAdd.isEmpty()) {
            return 0;
        }
        ClientGroup clientGroup = new ClientGroup(clientId, group);
        int added = 0;
        // Cap-and-log, do not throw: this runs on the LiteEventDispatcher background thread with no
        // client to respond to. Once the broker-wide quota is reached, stop auto-registering new
        // matches (existing subscriptions keep serving); the periodic re-expand retries next cycle
        // and picks up registration once room frees up. The guard precedes addLiteTopic so the
        // subscription's LiteTopicSet stays consistent with liteTopic2Group on early exit.
        long maxCount = brokerController.getBrokerConfig().getMaxLiteSubscriptionCount();
        for (String lmqName : toAdd) {
            if (getActiveSubscriptionNum() >= maxCount) {
                LOGGER.warn("reexpandWildcardPatterns capped at lite subscription quota {}, clientId:{}, group:{}, "
                    + "topic:{}, added:{}, skipped:{}", maxCount, clientId, group, topic, added, toAdd.size() - added);
                break;
            }
            thisSub.addLiteTopic(lmqName);
            if (addTopicGroup(clientGroup, lmqName)) {
                added++;
            }
        }
        if (added > 0) {
            LOGGER.info("reexpandWildcardPatterns, clientId:{}, group:{}, topic:{}, newly matched:{}",
                clientId, group, topic, added);
        }
        return added;
    }

    /**
     * Single-lmqName counterpart to {@link #reexpandWildcardPatterns(String)} for the
     * message-arriving dispatch path. When a message arrives on a lite-topic that no pattern-mode
     * client has been registered for yet (e.g. a newly-created lite-topic), enumerate the parent
     * topic's pattern-mode wildcard clients and register the ones whose stored patterns match this
     * lmqName's child. This avoids the O(M) {@code collectByParentTopic} scan that
     * {@code reexpandWildcardPatterns} performs, matching only against the single arriving lmqName.
     *
     * <p>Legacy wildcard clients are NOT touched here: they are already reachable via the synthetic
     * {@code topic@group} key in {@link #getAllSubscriber}. The periodic
     * {@code doFullDispatchForWildcardGroup} re-expand remains as a backstop.
     */
    @Override
    public int registerArrivingLmqForPatternClients(String lmqName) {
        String parentTopic = LiteUtil.getParentTopic(lmqName);
        String child = LiteUtil.getLiteTopic(lmqName);
        if (parentTopic == null || child == null) {
            return 0;
        }
        Set<String> wildcardGroups = wildcardGroupMap.get(parentTopic);
        if (wildcardGroups == null || wildcardGroups.isEmpty()) {
            return 0;
        }
        int added = 0;
        // Cap-and-log, do not throw: this runs on the message-arriving dispatch hot path with no
        // client to respond to (an exception here would abort per-message dispatch). Once the
        // broker-wide quota is reached, stop auto-registering the arriving lmqName; the periodic
        // doFullDispatchForWildcardGroup re-expand retries and picks it up once room frees up. The
        // guard precedes addLiteTopic so the subscription's LiteTopicSet stays consistent with
        // liteTopic2Group on early exit.
        long maxCount = brokerController.getBrokerConfig().getMaxLiteSubscriptionCount();
        for (String group : wildcardGroups) {
            if (getActiveSubscriptionNum() >= maxCount) {
                LOGGER.warn("registerArrivingLmqForPatternClients capped at lite subscription quota {}, "
                    + "topic:{}, lmqName:{}, added:{}", maxCount, parentTopic, lmqName, added);
                break;
            }
            for (String clientId : getAllClientIdByGroup(group)) {
                LiteSubscription thisSub = client2Subscription.get(clientId);
                if (thisSub == null || CollectionUtils.isEmpty(thisSub.getWildcardPatterns())) {
                    continue; // legacy or absent client
                }
                if (!LitePatternMatcher.matchesAny(thisSub.getWildcardPatterns(), child)) {
                    continue;
                }
                if (!liteLifecycleManager.isSubscriptionActive(parentTopic, lmqName)) {
                    continue; // mirror expandWildcardPatterns sharding guard
                }
                if (getActiveSubscriptionNum() >= maxCount) {
                    LOGGER.warn("registerArrivingLmqForPatternClients capped at lite subscription quota {}, "
                        + "topic:{}, lmqName:{}, added:{}", maxCount, parentTopic, lmqName, added);
                    break;
                }
                thisSub.addLiteTopic(lmqName);
                if (addTopicGroup(new ClientGroup(clientId, thisSub.getGroup()), lmqName)) {
                    added++;
                }
            }
        }
        if (added > 0) {
            LOGGER.info("registerArrivingLmqForPatternClients, topic:{}, lmqName:{}, newly registered:{}",
                parentTopic, lmqName, added);
        }
        return added;
    }

    @Override
    public void removeCompleteSubscription(String clientId) {
        clientChannels.remove(clientId);
        LiteSubscription thisSub = client2Subscription.remove(clientId);
        // Only clean tombstones for exclusive groups.
        if (thisSub == null || LiteMetadataUtil.isSubLiteExclusive(thisSub.getGroup(), brokerController)) {
            exclusiveEvictionTombstones.removeAllOf(clientId);
        }
        if (thisSub == null) {
            return;
        }
        LOGGER.info("removeCompleteSubscription, topic:{}, group:{}, clientId:{}", thisSub.getTopic(), thisSub.getGroup(), clientId);
        ClientGroup clientGroup = new ClientGroup(clientId, thisSub.getGroup());
        thisSub.getLiteTopicSet().forEach(lmqName -> {
            removeTopicGroup(clientGroup, lmqName, false);
        });
        // Pattern-mode wildcard groups are marked in wildcardGroupMap for fan-out enumeration but
        // register real lmqNames (so unmarkWildcardGroupIfNecessary, which parses the synthetic
        // topic@group key, never fires for them). Clean up explicitly when the last client leaves.
        if (CollectionUtils.isNotEmpty(thisSub.getWildcardPatterns())
            && getAllClientIdByGroup(thisSub.getGroup()).isEmpty()) {
            unmarkWildcardGroup(thisSub.getTopic(), thisSub.getGroup());
        }
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
        boolean isWildcardGroup = group != null && LiteMetadataUtil.isWildcardGroup(group, brokerController);

        if (group != null) {
            SubscriberWrapper.ListWrapper wrapper = new SubscriberWrapper.ListWrapper();
            // Pattern-mode wildcard groups register real lmqNames into liteTopic2Group (same as a
            // normal subscription), so the normal lookup finds their clients here.
            Set<ClientGroup> subscribers = liteTopic2Group.get(lmqName);
            if (subscribers != null) {
                wrapper.getClients().addAll(subscribers.stream()
                    .filter(clientGroup -> group.equals(clientGroup.group))
                    .collect(Collectors.toSet()));
            }
            // For a wildcard group, always merge clients from the synthetic topic@group key. Legacy
            // wildcard clients (receive-all) live there; pattern-mode clients never do (they register
            // real lmqNames, already merged above). This keeps delivery correct in mixed groups where
            // some clients use patterns and others are legacy — both sets are returned.
            if (isWildcardGroup) {
                List<ClientGroup> wildcardClients = getWildcardGroupClients(topic, group);
                if (CollectionUtils.isNotEmpty(wildcardClients)) {
                    wrapper.getClients().addAll(wildcardClients);
                }
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
            // Fan out wildcard groups via the synthetic topic@group key. Legacy wildcard clients are
            // only reachable there; pattern-mode wildcard clients are already in liteTopic2Group
            // (merged above) and absent from the synthetic key, so they are not double-counted. A
            // group may contain both kinds, so enumerate every wildcard group without skipping.
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

    protected boolean addTopicGroup(ClientGroup clientGroup, String lmqName) {
        Set<ClientGroup> topicGroupSet = liteTopic2Group
            .computeIfAbsent(lmqName, k -> ConcurrentHashMap.newKeySet());
        if (topicGroupSet.add(clientGroup)) {
            activeNum.incrementAndGet();
            invalidateWildcardCacheIfNecessary(clientGroup.group);
            for (LiteCtlListener listener : listeners) {
                listener.onRegister(clientGroup.clientId, clientGroup.group, lmqName);
            }
            return true;
        }
        return false;
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
            exclusiveEvictionTombstones.add(clientGroup.clientId, lmqName);
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

    private void unmarkWildcardGroup(String topic, String group) {
        wildcardGroupMap.computeIfPresent(topic, (k, v) -> {
            v.remove(group);
            return v.isEmpty() ? null : v;
        });
    }

    private void unmarkWildcardGroupIfNecessary(String lmqName) {
        if (!LiteUtil.isLiteTopicQueue(lmqName)) { // must be topic@group
            String[] topicAtGroup = StringUtils.split(lmqName, "@");
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

        int tombstoneSize = exclusiveEvictionTombstones.size();
        if (tombstoneSize > 0) {
            LOGGER.info("ExclusiveEvictionTombstones size: {}", tombstoneSize);
        }
    }

    @Override
    public boolean hasExclusiveEvictionTombstone(String clientId, String lmqName) {
        return exclusiveEvictionTombstones.contains(clientId, lmqName);
    }

}