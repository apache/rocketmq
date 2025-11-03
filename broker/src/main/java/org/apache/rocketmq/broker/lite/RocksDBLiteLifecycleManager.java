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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.RocksDBMessageStore;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueOffsetTable;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore;
import org.apache.rocketmq.tieredstore.TieredMessageStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class RocksDBLiteLifecycleManager extends AbstractLiteLifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    private Map<String, Long> maxCqOffsetTable;

    public RocksDBLiteLifecycleManager(BrokerController brokerController, LiteSharding liteSharding) {
        super(brokerController, liteSharding);
    }

    @Override
    public long getMaxOffsetInQueue(String lmqName) {
        return maxCqOffsetTable.getOrDefault(lmqName + "-0", -1L) + 1;
    }

    @Override
    public List<String> collectByParentTopic(String parentTopic) {
        if (StringUtils.isEmpty(parentTopic)) {
            return Collections.emptyList();
        }
        List<String> resultList = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iterator = maxCqOffsetTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            String queueAndQid = entry.getKey();
            String lmqName = queueAndQid.substring(0, queueAndQid.lastIndexOf("-"));
            if (LiteUtil.belongsTo(lmqName, parentTopic)) {
                resultList.add(lmqName);
            }
        }
        return resultList;
    }

    @Override
    public List<Pair<String, String>> collectExpiredLiteTopic() {
        List<Pair<String, String>> lmqToDelete = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iterator = maxCqOffsetTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            String queueAndQid = entry.getKey();
            String lmqName = queueAndQid.substring(0, queueAndQid.lastIndexOf("-"));
            String parentTopic = LiteUtil.getParentTopic(lmqName);
            if (null == parentTopic) {
                continue;
            }
            if (isLiteTopicExpired(parentTopic, lmqName, entry.getValue() + 1)) {
                lmqToDelete.add(new Pair<>(parentTopic, lmqName));
            }
        }
        return lmqToDelete;
    }

    @Override
    public void init() {
        super.init();
        if (messageStore instanceof TieredMessageStore) { // only support TieredMessageStore plugin
            messageStore = ((TieredMessageStore) messageStore).getDefaultStore();
        }
        if (!(messageStore instanceof RocksDBMessageStore)) {
            LOGGER.warn("init failed, not a RocksDB store. {}", messageStore.getClass());
            return; // startup with lite feature disabled
        }
        try {
            RocksDBConsumeQueueStore queueStore = (RocksDBConsumeQueueStore) messageStore.getQueueStore();
            RocksDBConsumeQueueOffsetTable cqOffsetTable = (RocksDBConsumeQueueOffsetTable) FieldUtils.readField(
                FieldUtils.getField(RocksDBConsumeQueueStore.class, "rocksDBConsumeQueueOffsetTable", true), queueStore);
            @SuppressWarnings("unchecked")
            ConcurrentMap<String, Long> innerMaxCqOffsetTable = (ConcurrentMap<String, Long>) FieldUtils.readField(
                FieldUtils.getField(RocksDBConsumeQueueOffsetTable.class, "topicQueueMaxCqOffset", true), cqOffsetTable);
            maxCqOffsetTable = Collections.unmodifiableMap(innerMaxCqOffsetTable);
        } catch (Exception e) {
            LOGGER.error("LiteLifecycleManager-init error", e);
        }
    }
}
