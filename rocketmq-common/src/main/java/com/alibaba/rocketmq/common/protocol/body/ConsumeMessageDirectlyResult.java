package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


public class ConsumeMessageDirectlyResult extends RemotingSerializable {
    private boolean order = false;
    private boolean autoCommit = true;
    private CMResult consumeResult;
    private String remark;
    private long spentTimeMills;


    public boolean isOrder() {
        return order;
    }


    public void setOrder(boolean order) {
        this.order = order;
    }


    public boolean isAutoCommit() {
        return autoCommit;
    }


    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }


    public String getRemark() {
        return remark;
    }


    public void setRemark(String remark) {
        this.remark = remark;
    }


    public CMResult getConsumeResult() {
        return consumeResult;
    }


    public void setConsumeResult(CMResult consumeResult) {
        this.consumeResult = consumeResult;
    }


    public long getSpentTimeMills() {
        return spentTimeMills;
    }


    public void setSpentTimeMills(long spentTimeMills) {
        this.spentTimeMills = spentTimeMills;
    }


    @Override
    public String toString() {
        return "ConsumeMessageDirectlyResult [order=" + order + ", autoCommit=" + autoCommit
                + ", consumeResult=" + consumeResult + ", remark=" + remark + ", spentTimeMills="
                + spentTimeMills + "]";
    }
}
