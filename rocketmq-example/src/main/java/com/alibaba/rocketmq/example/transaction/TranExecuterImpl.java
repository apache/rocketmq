package com.alibaba.rocketmq.example.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.Message;


public class TranExecuterImpl implements LocalTransactionExecuter {
    private int transactionStats = -1;// 0 send ,1 commit,2 rollback


    public int getTransactionStats() {
        return transactionStats;
    }


    public void setTransactionStats(int transactionStats) {
        this.transactionStats = transactionStats;
    }


    @Override
    public LocalTransactionState executeLocalTransactionBranch(Message msg) {
        if (transactionStats == 1) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
        else if (transactionStats == 2) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}
