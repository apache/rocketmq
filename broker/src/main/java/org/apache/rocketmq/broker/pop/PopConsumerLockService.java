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
package org.apache.rocketmq.broker.pop;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopConsumerLockService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    private final long timeout;
    private final ConcurrentMap<String /* groupId@topicId */, TimedLock> lockTable;

    public PopConsumerLockService(long timeout) {
        this.timeout = timeout;
        this.lockTable = new ConcurrentHashMap<>();
    }

    public boolean tryLock(String groupId, String topicId) {
        return Objects.requireNonNull(ConcurrentHashMapUtils.computeIfAbsent(lockTable,
            groupId + PopAckConstants.SPLIT + topicId, s -> new TimedLock())).tryLock();
    }

    public void unlock(String groupId, String topicId) {
        TimedLock lock = lockTable.get(groupId + PopAckConstants.SPLIT + topicId);
        if (lock != null) {
            lock.unlock();
        }
    }

    // For retry topics, should lock origin group and topic
    public boolean isLockTimeout(String groupId, String topicId) {
        topicId = KeyBuilder.parseNormalTopic(topicId, groupId);
        TimedLock lock = lockTable.get(groupId + PopAckConstants.SPLIT + topicId);
        return lock == null || System.currentTimeMillis() - lock.getLockTime() > timeout;
    }

    public void removeTimeout() {
        Iterator<Map.Entry<String, TimedLock>> iterator = lockTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, TimedLock> entry = iterator.next();
            if (System.currentTimeMillis() - entry.getValue().getLockTime() > timeout) {
                log.info("PopConsumerLockService remove timeout lock, " +
                    "key={}, locked={}", entry.getKey(), entry.getValue().lock.get());
                iterator.remove();
            }
        }
    }

    static class TimedLock {
        private volatile long lockTime;
        private final AtomicBoolean lock;

        public TimedLock() {
            this.lockTime = System.currentTimeMillis();
            this.lock = new AtomicBoolean(false);
        }

        public boolean tryLock() {
            if (lock.compareAndSet(false, true)) {
                this.lockTime = System.currentTimeMillis();
                return true;
            }
            return false;
        }

        public void unlock() {
            lock.set(false);
        }

        public long getLockTime() {
            return lockTime;
        }
    }
}