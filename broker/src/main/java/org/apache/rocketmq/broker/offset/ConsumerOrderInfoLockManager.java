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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ConsumerOrderInfoLockManager {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final Map<Key, Timeout> timeoutMap = new ConcurrentHashMap<>();
    private final Timer timer;
    private static final int TIMER_TICK_MS = 100;

    public ConsumerOrderInfoLockManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.timer = new HashedWheelTimer(
            new ThreadFactoryImpl("ConsumerOrderInfoLockManager_"),
            TIMER_TICK_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * when ConsumerOrderInfoManager load from disk, recover data
     */
    public void recover(Map<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo>> table) {
        if (!this.brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            return;
        }
        for (Map.Entry<String, ConcurrentHashMap<Integer, ConsumerOrderInfoManager.OrderInfo>> entry : table.entrySet()) {
            String topicAtGroup = entry.getKey();
            ConcurrentHashMap<Integer/*queueId*/, ConsumerOrderInfoManager.OrderInfo> qs = entry.getValue();
            String[] arrays = ConsumerOrderInfoManager.decodeKey(topicAtGroup);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];
            for (Map.Entry<Integer, ConsumerOrderInfoManager.OrderInfo> qsEntry : qs.entrySet()) {
                Long lockFreeTimestamp = qsEntry.getValue().getLockFreeTimestamp();
                if (lockFreeTimestamp == null || lockFreeTimestamp <= System.currentTimeMillis()) {
                    continue;
                }
                this.updateLockFreeTimestamp(topic, group, qsEntry.getKey(), lockFreeTimestamp);
            }
        }
    }

    public void updateLockFreeTimestamp(String topic, String group, int queueId, ConsumerOrderInfoManager.OrderInfo orderInfo) {
        this.updateLockFreeTimestamp(topic, group, queueId, orderInfo.getLockFreeTimestamp());
    }

    public void updateLockFreeTimestamp(String topic, String group, int queueId, Long lockFreeTimestamp) {
        if (!this.brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
            return;
        }
        if (lockFreeTimestamp == null) {
            return;
        }
        try {
            this.timeoutMap.compute(new Key(topic, group, queueId), (key, oldTimeout) -> {
                try {
                    long delay = lockFreeTimestamp - System.currentTimeMillis();
                    Timeout newTimeout = this.timer.newTimeout(new NotifyLockFreeTimerTask(key), delay, TimeUnit.MILLISECONDS);
                    if (oldTimeout != null) {
                        // cancel prev timerTask
                        oldTimeout.cancel();
                    }
                    return newTimeout;
                } catch (Exception e) {
                    POP_LOGGER.warn("add timeout task failed. key:{}, lockFreeTimestamp:{}", key, lockFreeTimestamp, e);
                    return oldTimeout;
                }
            });
        } catch (Exception e) {
            POP_LOGGER.error("unexpect error when updateLockFreeTimestamp. topic:{}, group:{}, queueId:{}, lockFreeTimestamp:{}",
                topic, group, queueId, lockFreeTimestamp, e);
        }
    }

    protected void notifyLockIsFree(Key key) {
        try {
            this.brokerController.getPopMessageProcessor().notifyLongPollingRequestIfNeed(key.topic, key.group, key.queueId);
        } catch (Exception e) {
            POP_LOGGER.error("unexpect error when notifyLockIsFree. key:{}", key, e);
        }
    }

    public void shutdown() {
        this.timer.stop();
    }

    @VisibleForTesting
    protected Map<Key, Timeout> getTimeoutMap() {
        return timeoutMap;
    }

    private class NotifyLockFreeTimerTask implements TimerTask {

        private final Key key;

        private NotifyLockFreeTimerTask(Key key) {
            this.key = key;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled() || !brokerController.getBrokerConfig().isEnableNotifyAfterPopOrderLockRelease()) {
                return;
            }
            notifyLockIsFree(key);
            timeoutMap.computeIfPresent(key, (key1, curTimeout) -> {
                if (curTimeout == timeout) {
                    // remove from map
                    return null;
                }
                return curTimeout;
            });
        }
    }

    private static class Key {
        private final String topic;
        private final String group;
        private final int queueId;

        public Key(String topic, String group, int queueId) {
            this.topic = topic;
            this.group = group;
            this.queueId = queueId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return queueId == key.queueId && Objects.equal(topic, key.topic) && Objects.equal(group, key.group);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(topic, group, queueId);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("topic", topic)
                .add("group", group)
                .add("queueId", queueId)
                .toString();
        }
    }
}
