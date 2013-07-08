package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.atomic.AtomicLong;


public class TransationStatsInfo {
    private String cliHostName;
    // 正在执行状态的事务
    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong prepareCount = new AtomicLong(0);


    public TransationStatsInfo(String cliHostName, long prepareCount, long commitCount, long rollbackCount) {
        this.cliHostName = cliHostName;
        this.commitCount.set(commitCount);
        this.rollbackCount.set(rollbackCount);
        this.prepareCount.set(prepareCount);
    }


    public boolean isNull() {
        return !((rollbackCount.get() + prepareCount.get() + commitCount.get() + prepareCount.get()) > 0);
    }


    public void dispose() {
        prepareCount.set(prepareCount.get() - commitCount.get() - rollbackCount.get());
        commitCount.set(0);
        rollbackCount.set(0);
    }


    public String tolog() {
        StringBuffer sb = new StringBuffer();
        sb.append("客户端事务消息统计").append(",");
        sb.append("客户端[").append(cliHostName).append("],");
        sb.append("PrepareCount[").append(prepareCount.get()).append("],");
        sb.append("CommitCount[").append(commitCount.get()).append("],");
        sb.append("RollbackCount[").append(rollbackCount.get()).append("]");
        return sb.toString();

    }


    public String getCliHostName() {
        return cliHostName;
    }

    public AtomicLong getRollbackCount() {
        return rollbackCount;
    }


    public AtomicLong getPrepareCount() {
        return prepareCount;
    }


    public void setCliHostName(String cliHostName) {
        this.cliHostName = cliHostName;
    }


    public AtomicLong getCommitCount() {
        return commitCount;
    }
}
