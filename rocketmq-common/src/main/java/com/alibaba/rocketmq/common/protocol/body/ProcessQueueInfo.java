package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.common.UtilAll;


public class ProcessQueueInfo {
    /**
     * 消费到哪里，提交的offset
     */
    private long commitOffset;

    /**
     * 缓存的消息Offset信息
     */
    private long cachedMsgMinOffset;
    private long cachedMsgMaxOffset;
    private int cachedMsgCount;

    /**
     * 正在事务中的消息
     */
    private long transactionMsgMinOffset;
    private long transactionMsgMaxOffset;
    private int transactionMsgCount;

    /**
     * 顺序消息的状态信息
     */
    private boolean locked;
    private long tryUnlockTimes;
    private long lastLockTimestamp;

    private boolean droped;
    private long lastPullTimestamp;
    private long lastConsumeTimestamp;


    public long getCommitOffset() {
        return commitOffset;
    }


    public void setCommitOffset(long commitOffset) {
        this.commitOffset = commitOffset;
    }


    public long getCachedMsgMinOffset() {
        return cachedMsgMinOffset;
    }


    public void setCachedMsgMinOffset(long cachedMsgMinOffset) {
        this.cachedMsgMinOffset = cachedMsgMinOffset;
    }


    public long getCachedMsgMaxOffset() {
        return cachedMsgMaxOffset;
    }


    public void setCachedMsgMaxOffset(long cachedMsgMaxOffset) {
        this.cachedMsgMaxOffset = cachedMsgMaxOffset;
    }


    public int getCachedMsgCount() {
        return cachedMsgCount;
    }


    public void setCachedMsgCount(int cachedMsgCount) {
        this.cachedMsgCount = cachedMsgCount;
    }


    public long getTransactionMsgMinOffset() {
        return transactionMsgMinOffset;
    }


    public void setTransactionMsgMinOffset(long transactionMsgMinOffset) {
        this.transactionMsgMinOffset = transactionMsgMinOffset;
    }


    public long getTransactionMsgMaxOffset() {
        return transactionMsgMaxOffset;
    }


    public void setTransactionMsgMaxOffset(long transactionMsgMaxOffset) {
        this.transactionMsgMaxOffset = transactionMsgMaxOffset;
    }


    public int getTransactionMsgCount() {
        return transactionMsgCount;
    }


    public void setTransactionMsgCount(int transactionMsgCount) {
        this.transactionMsgCount = transactionMsgCount;
    }


    public boolean isLocked() {
        return locked;
    }


    public void setLocked(boolean locked) {
        this.locked = locked;
    }


    public long getTryUnlockTimes() {
        return tryUnlockTimes;
    }


    public void setTryUnlockTimes(long tryUnlockTimes) {
        this.tryUnlockTimes = tryUnlockTimes;
    }


    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }


    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }


    public boolean isDroped() {
        return droped;
    }


    public void setDroped(boolean droped) {
        this.droped = droped;
    }


    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }


    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }


    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }


    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }


    @Override
    public String toString() {
        return "ProcessQueueInfo [commitOffset=" + commitOffset + ", cachedMsgMinOffset="
                + cachedMsgMinOffset + ", cachedMsgMaxOffset=" + cachedMsgMaxOffset + ", cachedMsgCount="
                + cachedMsgCount + ", transactionMsgMinOffset=" + transactionMsgMinOffset
                + ", transactionMsgMaxOffset=" + transactionMsgMaxOffset + ", transactionMsgCount="
                + transactionMsgCount + ", locked=" + locked + ", tryUnlockTimes=" + tryUnlockTimes
                + ", lastLockTimestamp=" + UtilAll.timeMillisToHumanString(lastLockTimestamp) + ", droped="
                + droped + ", lastPullTimestamp=" + UtilAll.timeMillisToHumanString(lastPullTimestamp)
                + ", lastConsumeTimestamp=" + UtilAll.timeMillisToHumanString(lastConsumeTimestamp) + "]";

    }
}
