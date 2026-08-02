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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.constant.LoggerName;
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

    public boolean tryLock(String key) {
        AtomicBoolean locked = new AtomicBoolean(false);
        lockTable.compute(key, (k, currentLock) -> {
            TimedLock lock = currentLock == null ? new TimedLock() : currentLock;
            locked.set(lock.tryLock());
            return lock;
        });
        return locked.get();
    }

    public boolean tryLock(String groupId, String topicId) {
        return tryLock(groupId + PopAckConstants.SPLIT + topicId);
    }

    public void unlock(String key) {
        TimedLock lock = lockTable.get(key);
        if (lock != null) {
            lock.unlock();
        }
    }

    public void unlock(String groupId, String topicId) {
        unlock(groupId + PopAckConstants.SPLIT + topicId);
    }

    // For retry topics, should lock origin group and topic
    public boolean isLockTimeout(String groupId, String topicId) {
        topicId = KeyBuilder.parseNormalTopic(topicId, groupId);
        TimedLock lock = lockTable.get(groupId + PopAckConstants.SPLIT + topicId);
        return lock == null || System.currentTimeMillis() - lock.getLockTime() > timeout;
    }

    public void removeTimeout() {
        for (Map.Entry<String, TimedLock> entry : lockTable.entrySet()) {
            if (System.currentTimeMillis() - entry.getValue().getLockTime() <= timeout) {
                continue;
            }

            TimedLock[] removedLock = new TimedLock[1];
            lockTable.computeIfPresent(entry.getKey(), (key, currentLock) -> {
                if (System.currentTimeMillis() - currentLock.getLockTime() > timeout) {
                    removedLock[0] = currentLock;
                    return null;
                }
                return currentLock;
            });
            if (removedLock[0] != null) {
                log.info("PopConsumerLockService remove timeout lock, " +
                    "key={}, locked={}", entry.getKey(), removedLock[0].lock.get());
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
