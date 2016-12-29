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
package org.apache.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    private final ConcurrentHashMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}", //
                            group, //
                            clientId, //
                            mq);
                    }

                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}", //
                            group, //
                            oldClientId, //
                            clientId, //
                            mq);
                        return true;
                    }

                    log.warn(
                        "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}", //
                        group, //
                        oldClientId, //
                        clientId, //
                        mq);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
        final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            } else {
                notLockedMqs.add(mq);
            }
        }

        if (!notLockedMqs.isEmpty()) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    for (MessageQueue mq : notLockedMqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                "tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}", //
                                group, //
                                clientId, //
                                mq);
                        }

                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                "tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}", //
                                group, //
                                oldClientId, //
                                clientId, //
                                mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                            "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}", //
                            group, //
                            oldClientId, //
                            clientId, //
                            mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }

        return lockedMqs;
    }

    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                    lockEntry.getClientId(),
                                    group,
                                    mq,
                                    clientId);
                            }
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                group,
                                mq,
                                clientId);
                        }
                    }
                } else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                        group,
                        clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    static class LockEntry {
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        public boolean isExpired() {
            boolean expired =
                (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
