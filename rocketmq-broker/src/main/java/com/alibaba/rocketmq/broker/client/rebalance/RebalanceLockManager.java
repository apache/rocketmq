/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 顺序消息争抢队列锁
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-26
 */
public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.RebalanceLockLoggerName);
    private final static long RebalanceLockMaxLiveTime = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    private final ConcurrentHashMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
            new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    class LockEntry {
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


        public boolean isExpired() {
            boolean expired =
                    (System.currentTimeMillis() - this.lastUpdateTimestamp) > RebalanceLockMaxLiveTime;

            return expired;
        }


        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }
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


    /**
     * 尝试锁队列
     * 
     * @return 是否lock成功
     */
    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {
        // 没有被锁住
        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<MessageQueue, LockEntry>(32);
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

                    // 锁已经过期，抢占它
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

                    // 锁被别的Client占用
                    log.warn(
                        "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}", //
                        group, //
                        oldClientId, //
                        clientId, //
                        mq);
                    return false;
                }
                finally {
                    this.lock.unlock();
                }
            }
            catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }
        // 已经锁住，尝试更新时间
        else {
            // isLocked 中已经更新了时间，这里不需要再更新
        }

        return true;
    }


    /**
     * 批量方式锁队列，返回锁定成功的队列集合
     * 
     * @return 是否lock成功
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
            final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        // 先通过不加锁的方式尝试查看哪些锁定，哪些没锁定
        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            }
            else {
                notLockedMqs.add(mq);
            }
        }

        if (!notLockedMqs.isEmpty()) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<MessageQueue, LockEntry>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    // 遍历没有锁住的队列
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

                        // 已经锁定
                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        // 锁已经过期，抢占它
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

                        // 锁被别的Client占用
                        log.warn(
                            "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}", //
                            group, //
                            oldClientId, //
                            clientId, //
                            mq);
                    }
                }
                finally {
                    this.lock.unlock();
                }
            }
            catch (InterruptedException e) {
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
                                log.info("unlockBatch, Group: {} {} {}",//
                                    group, //
                                    mq, //
                                    clientId);
                            }
                            else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",//
                                    lockEntry.getClientId(), //
                                    group, //
                                    mq, //
                                    clientId);
                            }
                        }
                        else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",//
                                group, //
                                mq, //
                                clientId);
                        }
                    }
                }
                else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",//
                        group, //
                        clientId);
                }
            }
            finally {
                this.lock.unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }
}
