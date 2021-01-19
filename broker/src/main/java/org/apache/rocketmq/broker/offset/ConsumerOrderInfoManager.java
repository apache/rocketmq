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

import com.alibaba.fastjson.annotation.JSONField;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ConsumerOrderInfoManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private static final long CLEAN_SPAN_FROM_LAST = 24 * 3600 * 1000;

    private ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    private transient BrokerController brokerController;

    public ConsumerOrderInfoManager() {
    }

    public ConsumerOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> getTable() {
        return table;
    }

    public void setTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> table) {
        this.table = table;
    }

    /**
     * not thread safe.
     *
     * @param topic
     * @param group
     * @param queueId
     * @param msgOffsetList
     */
    public int update(String topic, String group, int queueId, List<Long> msgOffsetList) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);

        // start is same.
        List<Long> simple = OrderInfo.simpleO(msgOffsetList);
        if (orderInfo != null && simple.get(0).equals(orderInfo.getO().get(0))) {
            if (simple.equals(orderInfo.getO())) {
                orderInfo.setC(orderInfo.getC() + 1);
            } else {
                // reset, because msgs are changed.
                orderInfo.setC(0);
            }
            orderInfo.setL(System.currentTimeMillis());
            orderInfo.setO(simple);
            orderInfo.setCm(0);
        } else {
            orderInfo = new OrderInfo();
            orderInfo.setO(simple);
            orderInfo.setL(System.currentTimeMillis());
            orderInfo.setC(0);
            orderInfo.setCm(0);

            qs.put(queueId, orderInfo);
        }

        return orderInfo.getC();
    }

    public boolean checkBlock(String topic, String group, int queueId, long invisibleTime) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
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

        boolean isBlock = System.currentTimeMillis() - orderInfo.getL() < invisibleTime;

        return isBlock && !orderInfo.isDone();
    }

    /**
     * @param topic
     * @param group
     * @param queueId
     * @param offset
     * @return -1 : illegal, -2 : no need commit, >= 0 : commit
     */
    public long commitAndNext(String topic, String group, int queueId, long offset) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            return offset + 1;
        }
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, offset, orderInfo);
            return offset + 1;
        }

        List<Long> o = orderInfo.getO();
        if (o == null || o.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, offset, orderInfo);
            return -1;
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
            if (offset == temp) {
                break;
            }
        }
        // not found
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, offset, orderInfo);
            return -1;
        }
        //set bit
        orderInfo.setCm(orderInfo.getCm() | (1L << i));
        if (orderInfo.isDone()) {
            if (size == 1) {
                return o.get(0) + 1;
            } else {
                return o.get(size - 1) + first + 1;
            }
        }
        return -2;
    }

    public OrderInfo get(String topic, String group, int queueId) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            return null;
        }

        return qs.get(queueId);
    }

    public int getConsumeCount(String topic, String group, int queueId) {
        OrderInfo orderInfo = get(topic, group, queueId);
        return orderInfo == null ? 0 : orderInfo.getC();
    }

    private void autoClean() {
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
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
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

            if (this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().get(group) == null) {
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

                if (System.currentTimeMillis() - qsEntry.getValue().getL() > CLEAN_SPAN_FROM_LAST) {
                    qsIterator.remove();
                    log.info("Not consume long time, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                    continue;
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
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        this.autoClean();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{\n").append("\t\"table\":{");
        Iterator<Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>>> iterator =
            this.table.entrySet().iterator();
        int count1 = 0;
        while (iterator.hasNext()) {
            Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> entry =
                iterator.next();
            if (count1 > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append("\n\t\t\"").append(entry.getKey()).append("\":{");
            Iterator<Map.Entry<Integer/*queueId*/, OrderInfo>> qsIterator = entry.getValue().entrySet().iterator();
            int count2 = 0;
            while (qsIterator.hasNext()) {
                Map.Entry<Integer/*queueId*/, OrderInfo> qsEntry = qsIterator.next();
                if (count2 > 0) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\n\t\t\t").append(qsEntry.getKey()).append(":")
                    .append(qsEntry.getValue().encode());
                count2++;
            }
            stringBuilder.append("\n\t\t}");
            count1++;
        }
        stringBuilder.append("\n\t}").append("\n}");
        return stringBuilder.toString();
    }

    public static class OrderInfo {
        /**
         * offset
         */
        private List<Long> o;
        /**
         * consumed count
         */
        private int c;
        /**
         * last consume timestamp
         */
        private long l;
        /**
         * commit offset bit
         */
        private long cm;

        public OrderInfo() {
        }

        public List<Long> getO() {
            return o;
        }

        public void setO(List<Long> o) {
            this.o = o;
        }

        public static List<Long> simpleO(List<Long> o) {
            List<Long> simple = new ArrayList<>();
            if (o.size() == 1) {
                simple.addAll(o);
                return simple;
            }
            Long first = o.get(0);
            simple.add(first);
            for (int i = 1; i < o.size(); i++) {
                simple.add(o.get(i) - first);
            }
            return simple;
        }

        public int getC() {
            return c;
        }

        public void setC(int c) {
            this.c = c;
        }

        public long getL() {
            return l;
        }

        public void setL(long l) {
            this.l = l;
        }

        public long getCm() {
            return cm;
        }

        public void setCm(long cm) {
            this.cm = cm;
        }

        @JSONField(serialize = false, deserialize = false)
        public boolean isDone() {
            if (o == null || o.isEmpty()) {
                return true;
            }
            int num = o.size();
            for (byte i = 0; i < num; i++) {
                if ((cm & (1L << i)) == 0) {
                    return false;
                }
            }
            return true;
        }

        @JSONField(serialize = false, deserialize = false)
        public String encode() {
            StringBuilder sb = new StringBuilder();
            sb.append("{").append("\"c\":").append(getC());
            sb.append(",").append("\"cm\":").append(getCm());
            sb.append(",").append("\"l\":").append(getL());
            sb.append(",").append("\"o\":[");
            if (getO() != null) {
                for (int i = 0; i < getO().size(); i++) {
                    sb.append(getO().get(i));
                    if (i < getO().size() - 1) {
                        sb.append(",");
                    }
                }
            }
            sb.append("]").append("}");
            return sb.toString();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("OrderInfo");
            sb.append("@").append(this.hashCode());
            sb.append("{o=").append(o);
            sb.append(", c=").append(c);
            sb.append(", l=").append(l);
            sb.append(", cm=").append(cm);
            sb.append(", d=").append(isDone());
            sb.append('}');
            return sb.toString();
        }
    }
}
