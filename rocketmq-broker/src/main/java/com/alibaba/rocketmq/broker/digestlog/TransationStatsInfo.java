package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.atomic.AtomicLong;


public class TransationStatsInfo {
    private String cliHostName;
    // 正在执行状态的事务
    private final AtomicLong beginCount = new AtomicLong(0);
    private final AtomicLong commitOneCount = new AtomicLong(0);
    private final AtomicLong commitTwoCount = new AtomicLong(0);
    private final AtomicLong forgetCount = new AtomicLong(0);
    private final AtomicLong rollbackCount = new AtomicLong(0);
    private final AtomicLong recoverCount = new AtomicLong(0);
    private final AtomicLong endCount = new AtomicLong(0);
    private final AtomicLong prepareCount = new AtomicLong(0);


    public TransationStatsInfo(String cliHostName, long beginCount, long commitOneCount, long commitTwoCount,
            long forgetCount, long rollbackCount, long recoverCount, long endCount, long prepareCount) {
        this.cliHostName = cliHostName;
        this.beginCount.set(beginCount);
        this.commitOneCount.set(commitOneCount);
        this.commitTwoCount.set(commitTwoCount);
        this.forgetCount.set(forgetCount);
        this.rollbackCount.set(rollbackCount);
        this.recoverCount.set(recoverCount);
        this.endCount.set(endCount);
        this.prepareCount.set(prepareCount);
    }


    public boolean isNull() {
        return !((beginCount.get() + commitOneCount.get() + commitTwoCount.get() + forgetCount.get()
                + rollbackCount.get() + recoverCount.get() + endCount.get() + prepareCount.get()) > 0);
    }


    public void dispose() {
        beginCount.set(beginCount.get() - commitOneCount.get() - commitTwoCount.get() - forgetCount.get()
                - rollbackCount.get() - recoverCount.get() - endCount.get() - prepareCount.get());
        commitOneCount.set(0);
        commitTwoCount.set(0);
        forgetCount.set(0);
        rollbackCount.set(0);
        recoverCount.set(0);
        endCount.set(0);
        prepareCount.set(0);
    }


    public String tolog() {
        StringBuffer sb = new StringBuffer();
        sb.append("客户端事务消息统计").append(",");
        sb.append("服务端[").append(System.getProperty("HOST_NAME")).append("],");
        sb.append("客户端[").append(cliHostName).append("],");
        sb.append("BeginCount[").append(beginCount).append("],");
        sb.append("CommitOneCount[").append(commitOneCount.get()).append("],");
        sb.append("CommitTwoCount[").append(commitTwoCount.get()).append("],");
        sb.append("ForgetCount[").append(forgetCount.get()).append("],");
        sb.append("RollbackCount[").append(rollbackCount.get()).append("],");
        sb.append("RecoverCount[").append(recoverCount.get()).append("],");
        sb.append("EndCount[").append(endCount.get()).append("],");
        sb.append("PrepareCount[").append(prepareCount.get()).append("]");
        return sb.toString();

    }


    public String getCliHostName() {
        return cliHostName;
    }


    public AtomicLong getBeginCount() {
        return beginCount;
    }


    public AtomicLong getCommitOneCount() {
        return commitOneCount;
    }


    public AtomicLong getCommitTwoCount() {
        return commitTwoCount;
    }


    public AtomicLong getForgetCount() {
        return forgetCount;
    }


    public AtomicLong getRollbackCount() {
        return rollbackCount;
    }


    public AtomicLong getRecoverCount() {
        return recoverCount;
    }


    public AtomicLong getEndCount() {
        return endCount;
    }


    public AtomicLong getPrepareCount() {
        return prepareCount;
    }


    public void setCliHostName(String cliHostName) {
        this.cliHostName = cliHostName;
    }
}
