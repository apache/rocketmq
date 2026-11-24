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

import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class LiteLifecycleManager extends AbstractLiteLifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    public LiteLifecycleManager(BrokerController brokerController, LiteSharding liteSharding) {
        super(brokerController, liteSharding);
    }

    @Override
    public long getMaxOffsetInQueue(String lmqName) {
        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(lmqName, 0);
        return consumeQueue != null ? consumeQueue.getMaxOffsetInQueue() : 0L;
    }

    @Override
    public void forEachLiteTopic(Function<Triple<String, Long, Long>, Boolean> function) {
        Iterator<Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> iterator =
            messageStore.getQueueStore().getConsumeQueueTable().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> entry = iterator.next();
            if (!LiteUtil.isLiteTopicQueue(entry.getKey())) {
                continue;
            }
            ConsumeQueueInterface consumeQueueInterface = entry.getValue().get(0);
            if (null == consumeQueueInterface) {
                continue;
            }
            Triple<String, Long, Long> triple = Triple.of(entry.getKey(), consumeQueueInterface.getMaxOffsetInQueue(), null);
            try {
                if (!function.apply(triple)) {
                    break;
                }
            } catch (Throwable e) {
                LOGGER.error("forEachLiteTopic error. {}", entry.getKey(), e);
                break;
            }
        }
    }
}
