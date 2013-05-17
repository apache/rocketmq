package com.alibaba.rocketmq.example.simple;

import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.MessageExt;

public class TransactionCheckListenerImpl implements TransactionCheckListener {

	@Override
	public boolean checkLocalTransactionBranchState(MessageExt msg) {
		// TODO Auto-generated method stub
		System.out.println("server checking TrMsg "+msg.toString());
		return false;
	}

}
