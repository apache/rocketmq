package com.alibaba.rocketmq.broker.client.rebalance;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-26
 */
public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.RebalanceLockLoggerName);
    private final static long RebalanceLockMaxLiveTime = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalanceLockMaxLiveTime", "60000"));
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
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
                this.readWriteLock.writeLock().lockInterruptibly();
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
                    this.readWriteLock.writeLock().unlock();
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


    public void unlock(final String group, final MessageQueue mq, final String clientId) {
        try {
            this.readWriteLock.writeLock().lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    LockEntry lockEntry = groupValue.get(mq);
                    if (null != lockEntry) {
                        boolean locked = lockEntry.isLocked(clientId);
                        groupValue.remove(mq);
                        log.info("unlock, Group: {} {} {} {}",//
                            group, //
                            mq, //
                            clientId, //
                            locked);
                    }
                }
            }
            finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }
}
