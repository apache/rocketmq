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

import com.alibaba.fastjson2.annotation.JSONField;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerOrderInfoManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private static final long CLEAN_SPAN_FROM_LAST = 24 * 3600 * 1000;

    private ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    private transient ConsumerOrderInfoLockManager consumerOrderInfoLockManager;
    private transient BrokerController brokerController;

    public ConsumerOrderInfoManager() {
    }

    public ConsumerOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.consumerOrderInfoLockManager = new ConsumerOrderInfoLockManager(brokerController);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> getTable() {
        return table;
    }

    public void setTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> table) {
        this.table = table;
    }

    protected static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    protected static String[] decodeKey(String key) {
        return key.split(TOPIC_GROUP_SEPARATOR);
    }

    private void updateLockFreeTimestamp(String topic, String group, int queueId, OrderInfo orderInfo) {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        }
    }

    /**
     * update the message list received
     *
     * @param isRetry is retry topic or not
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param popTime the time of pop message
     * @param invisibleTime invisible time
     * @param msgQueueOffsetList the queue offsets of messages
     * @param orderInfoBuilder will append order info to this builder
     */
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId, long popTime, long invisibleTime,
        List<Long> msgQueueOffsetList, StringBuilder orderInfoBuilder) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);

        if (orderInfo != null) {
            OrderInfo newOrderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
            newOrderInfo.mergeOffsetConsumedCount(orderInfo.attemptId, orderInfo.offsetList, orderInfo.offsetConsumedCount);

            orderInfo = newOrderInfo;
        } else {
            orderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
        }
        qs.put(queueId, orderInfo);

        Map<Long, Integer> offsetConsumedCount = orderInfo.offsetConsumedCount;
        int minConsumedTimes = Integer.MAX_VALUE;
        if (offsetConsumedCount != null) {
            Set<Long> offsetSet = offsetConsumedCount.keySet();
            for (Long offset : offsetSet) {
                Integer consumedTimes = offsetConsumedCount.getOrDefault(offset, 0);
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, consumedTimes);
                minConsumedTimes = Math.min(minConsumedTimes, consumedTimes);
            }

            if (offsetConsumedCount.size() != orderInfo.offsetList.size()) {
                // offsetConsumedCount only save messages which consumed count is greater than 0
                // if size not equal, means there are some new messages
                minConsumedTimes = 0;
            }
        } else {
            minConsumedTimes = 0;
        }

        // for compatibility
        // the old pop sdk use queueId to get consumedTimes from orderCountInfo
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic, queueId, minConsumedTimes);
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);

        if (orderInfo == null) {
            return false;
        }
        return orderInfo.needBlock(attemptId, invisibleTime);
    }

    public void clearBlock(String topic, String group, int queueId) {
        table.computeIfPresent(buildKey(topic, group), (key, val) -> {
            val.remove(queueId);
            return val;
        });
    }

    /**
     * mark message is consumed finished. return the consumer offset
     *
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param queueOffset queue offset of message
     * @return -1 : illegal, -2 : no need commit, >= 0 : commit
     */
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            return queueOffset + 1;
        }
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, queueOffset, orderInfo);
            return queueOffset + 1;
        }

        List<Long> o = orderInfo.offsetList;
        if (o == null || o.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, offset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return -2;
        }

        Long first = o.get(0);
        int i = 0, size = o.size();
        for (; i < size; i++) {
            long temp;
            if (i == 0) {
                temp = first;
            } else {
                temp = first + o.get(i);
            }
            if (queueOffset == temp) {
                break;
            }
        }
        // not found
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }
        //set bit
        orderInfo.setCommitOffsetBit(orderInfo.commitOffsetBit | (1L << i));
        long nextOffset = orderInfo.getNextOffset();

        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        return nextOffset;
    }

    /**
     * update next visible time of this message
     *
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param queueOffset queue offset of message
     * @param nextVisibleTime nex visible time
     */
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime, long nextVisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            log.warn("orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("orderInfo is null, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, queueOffset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return;
        }

        orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    protected void autoClean() {
        if (brokerController == null) {
            return;
        }
        Iterator<Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>>> iterator =
            this.table.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> entry =
                iterator.next();
            String topicAtGroup = entry.getKey();
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = entry.getValue();
            String[] arrays = decodeKey(topicAtGroup);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];

            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig == null) {
                iterator.remove();
                log.info("Topic not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            if (!this.brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(group)) {
                iterator.remove();
                log.info("Group not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            if (qs.isEmpty()) {
                iterator.remove();
                log.info("Order table is empty, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            Iterator<Map.Entry<Integer/*queueId*/, OrderInfo>> qsIterator = qs.entrySet().iterator();
            while (qsIterator.hasNext()) {
                Map.Entry<Integer/*queueId*/, OrderInfo> qsEntry = qsIterator.next();

                if (qsEntry.getKey() >= topicConfig.getReadQueueNums()) {
                    qsIterator.remove();
                    log.info("Queue not exist, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                    continue;
                }

                if (System.currentTimeMillis() - qsEntry.getValue().getLastConsumeTimestamp() > CLEAN_SPAN_FROM_LAST) {
                    qsIterator.remove();
                    log.info("Not consume long time, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                }
            }
        }
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        if (brokerController != null) {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        } else {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath("~");
        }
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOrderInfoManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOrderInfoManager.class);
            if (obj != null) {
                this.table = obj.table;
                if (this.consumerOrderInfoLockManager != null) {
                    this.consumerOrderInfoLockManager.recover(this.table);
                }
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        this.autoClean();
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public void shutdown() {
        if (this.consumerOrderInfoLockManager != null) {
            this.consumerOrderInfoLockManager.shutdown();
        }
    }

    @VisibleForTesting
    protected ConsumerOrderInfoLockManager getConsumerOrderInfoLockManager() {
        return consumerOrderInfoLockManager;
    }

    public static class OrderInfo {
        private long popTime;
        /**
         * the invisibleTime when pop message
         */
        @JSONField(name = "i")
        private Long invisibleTime;
        /**
         * offset
         * offsetList[0] is the queue offset of message
         * offsetList[i] (i > 0) is the distance between current message and offsetList[0]
         */
        @JSONField(name = "o")
        private List<Long> offsetList;
        /**
         * next visible timestamp for message
         * key: message queue offset
         */
        @JSONField(name = "ot")
        private Map<Long, Long> offsetNextVisibleTime;
        /**
         * message consumed count for offset
         * key: message queue offset
         */
        @JSONField(name = "oc")
        private Map<Long, Integer> offsetConsumedCount;
        /**
         * last consume timestamp
         */
        @JSONField(name = "l")
        private long lastConsumeTimestamp;
        /**
         * commit offset bit
         */
        @JSONField(name = "cm")
        private long commitOffsetBit;
        @JSONField(name = "a")
        private String attemptId;

        public OrderInfo() {
        }

        public OrderInfo(String attemptId, long popTime, long invisibleTime, List<Long> queueOffsetList, long lastConsumeTimestamp,
            long commitOffsetBit) {
            this.popTime = popTime;
            this.invisibleTime = invisibleTime;
            this.offsetList = buildOffsetList(queueOffsetList);
            this.lastConsumeTimestamp = lastConsumeTimestamp;
            this.commitOffsetBit = commitOffsetBit;
            this.attemptId = attemptId;
        }

        public List<Long> getOffsetList() {
            return offsetList;
        }

        public void setOffsetList(List<Long> offsetList) {
            this.offsetList = offsetList;
        }

        public long getLastConsumeTimestamp() {
            return lastConsumeTimestamp;
        }

        public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
            this.lastConsumeTimestamp = lastConsumeTimestamp;
        }

        public long getCommitOffsetBit() {
            return commitOffsetBit;
        }

        public void setCommitOffsetBit(long commitOffsetBit) {
            this.commitOffsetBit = commitOffsetBit;
        }

        public long getPopTime() {
            return popTime;
        }

        public void setPopTime(long popTime) {
            this.popTime = popTime;
        }

        public Long getInvisibleTime() {
            return invisibleTime;
        }

        public void setInvisibleTime(Long invisibleTime) {
            this.invisibleTime = invisibleTime;
        }

        public Map<Long, Long> getOffsetNextVisibleTime() {
            return offsetNextVisibleTime;
        }

        public void setOffsetNextVisibleTime(Map<Long, Long> offsetNextVisibleTime) {
            this.offsetNextVisibleTime = offsetNextVisibleTime;
        }

        public Map<Long, Integer> getOffsetConsumedCount() {
            return offsetConsumedCount;
        }

        public void setOffsetConsumedCount(Map<Long, Integer> offsetConsumedCount) {
            this.offsetConsumedCount = offsetConsumedCount;
        }

        public String getAttemptId() {
            return attemptId;
        }

        public void setAttemptId(String attemptId) {
            this.attemptId = attemptId;
        }

        public static List<Long> buildOffsetList(List<Long> queueOffsetList) {
            List<Long> simple = new ArrayList<>();
            if (queueOffsetList.size() == 1) {
                simple.addAll(queueOffsetList);
                return simple;
            }
            Long first = queueOffsetList.get(0);
            simple.add(first);
            for (int i = 1; i < queueOffsetList.size(); i++) {
                simple.add(queueOffsetList.get(i) - first);
            }
            return simple;
        }

        @JSONField(serialize = false, deserialize = false)
        public boolean needBlock(String attemptId, long currentInvisibleTime) {
            if (offsetList == null || offsetList.isEmpty()) {
                return false;
            }
            if (this.attemptId != null && this.attemptId.equals(attemptId)) {
                return false;
            }
            int num = offsetList.size();
            int i = 0;
            if (this.invisibleTime == null || this.invisibleTime <= 0) {
                this.invisibleTime = currentInvisibleTime;
            }
            long currentTime = System.currentTimeMillis();
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    long nextVisibleTime = popTime + invisibleTime;
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }
                    if (currentTime < nextVisibleTime) {
                        return true;
                    }
                }
            }
            return false;
        }

        @JSONField(serialize = false, deserialize = false)
        public Long getLockFreeTimestamp() {
            if (offsetList == null || offsetList.isEmpty()) {
                return null;
            }
            int num = offsetList.size();
            int i = 0;
            long currentTime = System.currentTimeMillis();
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    if (invisibleTime == null || invisibleTime <= 0) {
                        return null;
                    }
                    long nextVisibleTime = popTime + invisibleTime;
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }
                    if (currentTime < nextVisibleTime) {
                        return nextVisibleTime;
                    }
                }
            }
            return currentTime;
        }

        @JSONField(serialize = false, deserialize = false)
        public void updateOffsetNextVisibleTime(long queueOffset, long nextVisibleTime) {
            if (this.offsetNextVisibleTime == null) {
                this.offsetNextVisibleTime = new HashMap<>();
            }
            this.offsetNextVisibleTime.put(queueOffset, nextVisibleTime);
        }

        @JSONField(serialize = false, deserialize = false)
        public long getNextOffset() {
            if (offsetList == null || offsetList.isEmpty()) {
                return -2;
            }
            int num = offsetList.size();
            int i = 0;
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    break;
                }
            }
            if (i == num) {
                // all ack
                return getQueueOffset(num - 1) + 1;
            }
            return getQueueOffset(i);
        }

        /**
         * convert the offset at the index of offsetList to queue offset
         *
         * @param offsetIndex the index of offsetList
         * @return queue offset of message
         */
        @JSONField(serialize = false, deserialize = false)
        public long getQueueOffset(int offsetIndex) {
            return getQueueOffset(this.offsetList, offsetIndex);
        }

        protected static long getQueueOffset(List<Long> offsetList, int offsetIndex) {
            if (offsetIndex == 0) {
                return offsetList.get(0);
            }
            return offsetList.get(0) + offsetList.get(offsetIndex);
        }

        @JSONField(serialize = false, deserialize = false)
        public boolean isNotAck(int offsetIndex) {
            return (commitOffsetBit & (1L << offsetIndex)) == 0;
        }

        /**
         * calculate message consumed count of each message, and put nonzero value into offsetConsumedCount
         *
         * @param prevOffsetConsumedCount the offset list of message
         */
        @JSONField(serialize = false, deserialize = false)
        public void mergeOffsetConsumedCount(String preAttemptId, List<Long> preOffsetList, Map<Long, Integer> prevOffsetConsumedCount) {
            Map<Long, Integer> offsetConsumedCount = new HashMap<>();
            if (prevOffsetConsumedCount == null) {
                prevOffsetConsumedCount = new HashMap<>();
            }
            if (preAttemptId != null && preAttemptId.equals(this.attemptId)) {
                this.offsetConsumedCount = prevOffsetConsumedCount;
                return;
            }
            Set<Long> preQueueOffsetSet = new HashSet<>();
            for (int i = 0; i < preOffsetList.size(); i++) {
                preQueueOffsetSet.add(getQueueOffset(preOffsetList, i));
            }
            for (int i = 0; i < offsetList.size(); i++) {
                long queueOffset = this.getQueueOffset(i);
                if (preQueueOffsetSet.contains(queueOffset)) {
                    int count = 1;
                    Integer preCount = prevOffsetConsumedCount.get(queueOffset);
                    if (preCount != null) {
                        count = preCount + 1;
                    }
                    offsetConsumedCount.put(queueOffset, count);
                }
            }
            this.offsetConsumedCount = offsetConsumedCount;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("popTime", popTime)
                .add("invisibleTime", invisibleTime)
                .add("offsetList", offsetList)
                .add("offsetNextVisibleTime", offsetNextVisibleTime)
                .add("offsetConsumedCount", offsetConsumedCount)
                .add("lastConsumeTimestamp", lastConsumeTimestamp)
                .add("commitOffsetBit", commitOffsetBit)
                .add("attemptId", attemptId)
                .toString();
        }
    }
}
