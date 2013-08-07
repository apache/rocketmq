package com.alibaba.rocketmq.test.integration.consumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.test.integration.BaseTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * description for com.alibaba.rocketmq.test.integration.consumer.PushConsumerTest
 * User: manhong.yqd
 */
public class PushConsumerTest extends BaseTest {
	private DefaultMQPushConsumer consumer;

	@Before
	public void before() throws Exception {
		consumer = new DefaultMQPushConsumer(consumerGroup);
		consumer.setInstanceName("DEFAULT");
		consumer.setMessageModel(MessageModel.CLUSTERING);
		consumer.subscribe(topic, "TagA || TagC || TagD");
	}

	@Test
	public void pushConsumer() throws MQClientException {
		MessageListener listener = new MessageListenerConcurrently() {
			AtomicLong consumeTimes = new AtomicLong(0);

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
				this.consumeTimes.incrementAndGet();
				if ((this.consumeTimes.get() % 2) == 0) {
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				} else if ((this.consumeTimes.get() % 3) == 0) {
					context.setDelayLevelWhenNextConsume(5);
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		};
		consumer.registerMessageListener(listener);

		consumer.start();
		System.out.println("Consumer Started.");

		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
