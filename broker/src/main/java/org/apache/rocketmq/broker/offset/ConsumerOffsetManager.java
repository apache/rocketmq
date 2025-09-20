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
package org.apache.rocketmq.broker.offset;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import static org.apache.rocketmq.common.MixAll.BROADCAST_KEY;

public class ConsumerOffsetManager extends ConfigManager {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public static final String TOPIC_GROUP_SEPARATOR = "@";

    protected DataVersion dataVersion = new DataVersion();

    protected ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>(512);

    protected final ConcurrentMap<String, ConcurrentMap<Integer, Long>> resetOffsetTable = new ConcurrentHashMap<>(512);

    private final ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> pullOffsetTable = new ConcurrentHashMap<>(512);

    protected transient BrokerController brokerController;

    private final transient AtomicLong versionChangeCounter = new AtomicLong(0);

    public ConsumerOffsetManager() {
    }

    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    protected void removeConsumerOffset(String topicAtGroup) {

    }

    public void cleanOffset(String group) {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (validateOffsetTableKey(topicAtGroup) && group.equals(arrays[1])) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    LOG.warn("Clean group's offset, {}, {}", topicAtGroup, next.getValue());
                }
            }
        }
    }

    public void cleanOffsetByTopic(String topic) {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(topic)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (validateOffsetTableKey(topicAtGroup) && topic.equals(arrays[0])) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    pullOffsetTable.remove(topicAtGroup);
                    resetOffsetTable.remove(topicAtGroup);
                    LOG.warn("Clean topic's offset, {}, {}", topicAtGroup, next.getValue());
                }
            }
        }
    }

    public void scanUnsubscribedTopic() {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (validateOffsetTableKey(topicAtGroup)) {
                String topic = arrays[0];
                String group = arrays[1];

                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic) && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    it.remove();
                    removeConsumerOffset(topicAtGroup);
                    LOG.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();

        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, next.getKey());
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<>();
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (validateOffsetTableKey(topicAtGroup)) {
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }

    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (validateOffsetTableKey(topicAtGroup)) {
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }

        return groups;
    }

    public Map<String, Set<String>> getGroupTopicMap() {
        Map<String, Set<String>> retMap = new HashMap<>(128);

        for (String key : this.offsetTable.keySet()) {
            String[] arr = key.split(TOPIC_GROUP_SEPARATOR);
            if (validateOffsetTableKey(key)) {
                String topic = arr[0];
                String group = arr[1];

                Set<String> topics = retMap.get(group);
                if (topics == null) {
                    topics = new HashSet<>(8);
                    retMap.put(group, topics);
                }

                topics.add(topic);
            }
        }

        return retMap;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }

    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                LOG.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
        if (versionChangeCounter.incrementAndGet() % brokerController.getBrokerConfig().getConsumerOffsetUpdateVersionStep() == 0) {
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
        }
    }

    public void commitPullOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = this.pullOffsetTable.computeIfAbsent(key, k -> new ConcurrentHashMap<>(32));
        map.put(queueId, offset);
    }

    /**
     * If the target queue has temporary reset offset, return the reset-offset.
     * Otherwise, return the current consume offset in the offset store.
     *
     * @param group   Consumer group
     * @param topic   Topic
     * @param queueId Queue ID
     * @return current consume offset or reset offset if there were one.
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;

        if (this.brokerController.getBrokerConfig().isUseServerSideResetOffset()) {
            Map<Integer, Long> reset = resetOffsetTable.get(key);
            if (null != reset && reset.containsKey(queueId)) {
                return reset.get(queueId);
            }
        }

        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null) {
                return offset;
            }
        }

        return -1L;
    }

    /**
     * Query pull offset in pullOffsetTable
     *
     * @param group   Consumer group
     * @param topic   Topic
     * @param queueId Queue ID
     * @return latest pull offset of consumer group
     */
    public long queryPullOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        Long offset = null;

        ConcurrentMap<Integer, Long> map = this.pullOffsetTable.get(key);
        if (null != map) {
            offset = map.get(queueId);
        }

        if (offset == null) {
            offset = queryOffset(group, topic, queueId);
        }

        return offset;
    }

    public void clearPullOffset(final String group, final String topic) {
        this.pullOffsetTable.remove(topic + TOPIC_GROUP_SEPARATOR + group);
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.setOffsetTable(obj.getOffsetTable());
                this.dataVersion = obj.dataVersion;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {

        Map<Integer, Long> queueMinOffset = new HashMap<>();
        Set<String> topicGroups = this.offsetTable.keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    String topicAtGroup = it.next();
                    if (validateOffsetTableKey(topicAtGroup) && group.equals(topicAtGroup.split(TOPIC_GROUP_SEPARATOR)[1])) {
                        it.remove();
                        removeConsumerOffset(topicAtGroup);
                    }
                }
            }
        }

        for (Map.Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            if (validateOffsetTableKey(topicGroup) && topic.equals(topicGroupArr[0])) {
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, entry.getKey());
                    if (entry.getValue() >= minOffset) {
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        } else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }

        }
        return queueMinOffset;
    }

    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<>(offsets));
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public boolean loadDataVersion() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null) {
                ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
                if (obj != null) {
                    this.dataVersion = obj.dataVersion;
                }
                LOG.info("load consumer offset dataVersion success,{},{} ", fileName, jsonString);
            }
            return true;
        } catch (Exception e) {
            LOG.error("load consumer offset dataVersion failed " + fileName, e);
            return false;
        }
    }

    public void removeOffset(final String group) {
        Function<Iterator<Entry<String, ConcurrentMap<Integer, Long>>>, Boolean> deleteFunction = it -> {
            boolean removed = false;
            while (it.hasNext()) {
                Entry<String, ConcurrentMap<Integer, Long>> entry = it.next();
                String topicAtGroup = entry.getKey();
                if (topicAtGroup.contains(group)) {
                    String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                    if (validateOffsetTableKey(topicAtGroup) && group.equals(arrays[1])) {
                        it.remove();
                        removeConsumerOffset(topicAtGroup);
                        removed = true;
                    }
                }
            }
            return removed;
        };

        boolean clearOffset = deleteFunction.apply(this.offsetTable.entrySet().iterator());
        boolean clearReset = deleteFunction.apply(this.resetOffsetTable.entrySet().iterator());
        boolean clearPull = deleteFunction.apply(this.pullOffsetTable.entrySet().iterator());

        LOG.info("Consumer offset manager clean group offset, groupName={}, " + "offsetTable={}, resetOffsetTable={}, pullOffsetTable={}", group, clearOffset, clearReset, clearPull);
    }

    public void assignResetOffset(String topic, String group, int queueId, long offset) {
        if (Strings.isNullOrEmpty(topic) || Strings.isNullOrEmpty(group) || queueId < 0 || offset < 0) {
            LOG.warn("Illegal arguments when assigning reset offset. Topic={}, group={}, queueId={}, offset={}", topic, group, queueId, offset);
            return;
        }

        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        resetOffsetTable.computeIfAbsent(key, k -> Maps.newConcurrentMap()).put(queueId, offset);
        LOG.debug("Reset offset OK. Topic={}, group={}, queueId={}, resetOffset={}", topic, group, queueId, offset);

        // Two things are important here:
        // 1, currentOffsetMap might be null if there is no previous records;
        // 2, Our overriding here may get overridden by the client instantly in concurrent cases; But it still makes
        // sense in cases like clients are offline.
        offsetTable.computeIfAbsent(key, k -> Maps.newConcurrentMap()).put(queueId, offset);
    }

    public boolean hasOffsetReset(String topic, String group, int queueId) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = resetOffsetTable.get(key);
        if (null == map) {
            return false;
        }
        return map.containsKey(queueId);
    }

    public Long queryThenEraseResetOffset(String topic, String group, Integer queueId) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = resetOffsetTable.get(key);
        if (null == map) {
            return null;
        } else {
            return map.remove(queueId);
        }
    }

    public boolean validateOffsetTableKey(String key) {
        String[] arr = key.split(TOPIC_GROUP_SEPARATOR);
        return arr.length == 2 || arr.length == 3 && BROADCAST_KEY.equals(arr[2]);
    }
}