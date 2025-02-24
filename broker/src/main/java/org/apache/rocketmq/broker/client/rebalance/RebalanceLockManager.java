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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<>(1024);

    public boolean isLockAllExpired(final String group) {
        final ConcurrentHashMap<MessageQueue, LockEntry> lockEntryMap = mqLockTable.get(group);
        if (null == lockEntryMap) {
            return true;
        }
        for (LockEntry entry : lockEntryMap.values()) {
            if (!entry.isExpired()) {
                return false;
            }
        }
        return true;
    }

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.computeIfAbsent(group,
                        k -> new ConcurrentHashMap<>(32));

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        groupValue.put(mq, lockEntry);
                        log.info(
                            "RebalanceLockManager#tryLock: lock a message queue which has not been locked yet, "
                                + "group={}, clientId={}, mq={}", group, clientId, mq);
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "RebalanceLockManager#tryLock: try to lock a expired message queue, group={}, mq={}, old "
                                + "client id={}, new client id={}", group, mq, oldClientId, clientId);
                        return true;
                    }

                    log.warn(
                        "RebalanceLockManager#tryLock: message queue has been locked by other client, group={}, "
                            + "mq={}, locked client id={}, current client id={}", group, mq, oldClientId, clientId);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("RebalanceLockManager#tryLock: unexpected error, group={}, mq={}, clientId={}", group, mq,
                    clientId, e);
            }
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
        Set<MessageQueue> lockedMqs = new HashSet<>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<>(mqs.size());

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
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.computeIfAbsent(group,
                        k -> new ConcurrentHashMap<>(32));

                    for (MessageQueue mq : notLockedMqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            groupValue.put(mq, lockEntry);
                            lockedMqs.add(mq);
                            log.info(
                                "RebalanceLockManager#tryLockBatch: lock a message which has not been locked yet, "
                                    + "group={}, clientId={}, mq={}", group, clientId, mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                "RebalanceLockManager#tryLockBatch: try to lock a expired message queue, group={}, "
                                    + "mq={}, old client id={}, new client id={}", group, mq, oldClientId, clientId);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                            "RebalanceLockManager#tryLockBatch: message queue has been locked by other client, "
                                + "group={}, mq={}, locked client id={}, current client id={}", group, mq, oldClientId,
                            clientId);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("RebalanceLockManager#tryBatch: unexpected error, group={}, mqs={}, clientId={}", group, mqs,
                    clientId, e);
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
                                log.info("RebalanceLockManager#unlockBatch: unlock mq, group={}, clientId={}, mqs={}",
                                    group, clientId, mq);
                            } else {
                                log.warn(
                                    "RebalanceLockManager#unlockBatch: mq locked by other client, group={}, locked "
                                        + "clientId={}, current clientId={}, mqs={}", group, lockEntry.getClientId(),
                                    clientId, mq);
                            }
                        } else {
                            log.warn("RebalanceLockManager#unlockBatch: mq not locked, group={}, clientId={}, mq={}",
                                group, clientId, mq);
                        }
                    }
                } else {
                    log.warn("RebalanceLockManager#unlockBatch: group not exist, group={}, clientId={}, mqs={}", group,
                        clientId, mqs);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("RebalanceLockManager#unlockBatch: unexpected error, group={}, mqs={}, clientId={}", group, mqs,
                clientId);
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
