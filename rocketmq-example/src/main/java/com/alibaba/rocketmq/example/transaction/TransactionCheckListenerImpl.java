package com.alibaba.rocketmq.example.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.MessageExt;


public class TransactionCheckListenerImpl implements TransactionCheckListener {

    @Override
    public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
        System.out.println("server checking TrMsg " + msg.toString());
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
