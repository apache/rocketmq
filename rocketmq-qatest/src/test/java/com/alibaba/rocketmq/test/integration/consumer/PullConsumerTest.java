package com.alibaba.rocketmq.test.integration.consumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.test.integration.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * description for PullConsumerTest
 * User: manhong.yqd
 */
public class PullConsumerTest extends BaseTest {
	private DefaultMQPullConsumer consumer;

	@Before
	public void before() throws Exception {
		consumer = initConsumer("DEFAULT");
	}

	/**
	 * 消费广播消息
	 *
	 * @throws MQClientException
	 * @throws RemotingException
	 * @throws MQBrokerException
	 * @throws InterruptedException
	 */
	@Test
	public void broadCastMessage() throws MQClientException,
			RemotingException, MQBrokerException, InterruptedException {
		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);

		System.out.println("mqs:" + mqs.size());
		while (true) {
			for (MessageQueue mq : mqs) {
				PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq, false), 10);
				putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
				firePullResult(pullResult);
			}
			TimeUnit.SECONDS.sleep(2);
		}
	}

	/**
	 * 消费集群消息
	 *
	 * @throws MQClientException
	 * @throws RemotingException
	 * @throws InterruptedException
	 * @throws MQBrokerException
	 */
	@Test
	public void clusterMessage() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
		while (true) {
			Set<MessageQueue> mqs = consumer.fetchMessageQueuesInBalance(topic);
			System.out.println("mqs:" + mqs.size());
			for (MessageQueue mq : mqs) {
				PullResult pullResult = consumer.pull(mq, null, getMessageQueueOffset(mq, false), 10);
				putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
				firePullResult(pullResult);
			}
			TimeUnit.SECONDS.sleep(2);
		}
	}

	@Test
	// 根据消息ID查询消息
	public void searchByMessageId() throws MQClientException, RemotingException, MQBrokerException,
			InterruptedException, UnknownHostException {
		ByteBuffer byteBufferMsgId = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
		ByteBuffer addr = ByteBuffer.allocate(8);
		addr.put(InetAddress.getByName("10.232.25.81").getAddress());
		addr.putInt(10911);
		long offset = 0;
		while (true) {
			Set<MessageQueue> mqs = consumer.fetchMessageQueuesInBalance(topic);
			System.out.println("mqs:" + mqs.size());
			for (MessageQueue mq : mqs) {
				addr.flip();
				String msgId = MessageDecoder.createMessageId(byteBufferMsgId, addr, 1);
				MessageExt data = consumer.viewMessage(msgId);
				System.out.println(data);
			}
			TimeUnit.SECONDS.sleep(2);
		}
	}

	@After
	public void after() throws Exception {
		consumer.shutdown();
		Thread.sleep(2000);
	}

	private void firePullResult(PullResult pullResult) {
		switch (pullResult.getPullStatus()) {
			case FOUND:
				for (MessageExt mex : pullResult.getMsgFoundList()) {
					System.out.println(mex);
				}
				break;
			case NO_MATCHED_MSG:
				break;
			case NO_NEW_MSG:
				break;
			case OFFSET_ILLEGAL:
				break;
			default:
				break;
		}
	}

	private void putMessageQueueOffset(MessageQueue mq, long offset) throws MQClientException {
		consumer.updateConsumeOffset(mq, offset);
	}

	private long getMessageQueueOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
		long offset = consumer.fetchConsumeOffset(mq, true);
		offset = (offset < 0 ? 0 : offset);
		System.out.println("offset:" + offset);
		return offset;
	}

	private DefaultMQPullConsumer initConsumer(String instanceName) throws MQClientException, InterruptedException {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
		consumer.setMessageModel(messageModel);
		consumer.setInstanceName(instanceName);
		MessageQueueListener listener = new MessageQueueListener() {
			@Override
			public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
				System.out.println("Topic=" + topic);
				System.out.println("mqAll:" + mqAll);
				System.out.println("mqDivided:" + mqDivided);
			}
		};
		consumer.registerMessageQueueListener("TopicTest", listener);
		consumer.start();
		Thread.sleep(2000);
		return consumer;
	}
}
