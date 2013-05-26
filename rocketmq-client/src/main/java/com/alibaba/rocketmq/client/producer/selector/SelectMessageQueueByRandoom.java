package com.alibaba.rocketmq.client.producer.selector;

import java.util.List;
import java.util.Random;

import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.common.MessageQueue;

public class SelectMessageQueueByRandoom implements MessageQueueSelector {

	@Override
	public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
		// TODO Auto-generated method stub
		Random random = new Random(mqs.size());
		return mqs.get(random.nextInt());
	}

}
