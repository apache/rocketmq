package com.alibaba.rocketmq.example.transaction;

import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.Message;


public class TransactionExecuterImpl implements LocalTransactionExecuter {
    private AtomicInteger transactionIndex = new AtomicInteger(1);


    @Override
    public LocalTransactionState executeLocalTransactionBranch(Message msg) {
        int value = transactionIndex.getAndIncrement();

        if ((value % 2) == 0) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
        else if ((value % 3) == 0) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.UNKNOW;
    }
}
