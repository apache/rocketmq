package com.alibaba.rocketmq.example.transaction;

import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;


public class TransactionExecuterImpl implements LocalTransactionExecuter {
    private AtomicInteger transactionIndex = new AtomicInteger(1);


    @Override
    public LocalTransactionState executeLocalTransactionBranch(Message msg) {
        int value = transactionIndex.getAndIncrement();

        if (value == 0) {
            throw new RuntimeException("Could not find db");
        }
        else if ((value % 5) == 0) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        else if ((value % 4) == 0) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }

        return LocalTransactionState.UNKNOW;
    }
}
