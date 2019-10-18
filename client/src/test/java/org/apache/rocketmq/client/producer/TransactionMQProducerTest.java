package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @program: rocketmq
 * @description: 事务消息测试类
 * @author: TonyStark888
 * @create: 2019-10-18 11:22
 **/
public class TransactionMQProducerTest {
	private TransactionMQProducer producer;
	private String producerGroupPrefix = "Transaction_PID";

	@Before
	public void init() throws Exception {
		String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
		producer = new TransactionMQProducer(producerGroupTemp);
		producer.setNamesrvAddr("10.0.133.29:9876");
		producer.setTransactionListener(new DelayTimeLevelTransactionListener());
		producer.start();
	}

	@After
	public void terminate() {
		producer.shutdown();
	}

	@Test
	public void testSendMessage() throws InterruptedException, RemotingException, MQBrokerException {
		try {
			Message message = new Message("TransactionTopic", "transactionTest", "msg-1", ("HelloTime").getBytes());
			SendResult result = producer.sendMessageInTransaction(message, "HelloTime");
			System.out.printf("Topic:%s send success, misId is:%s%n", message.getTopic(), result.getMsgId());

			// 挂起5分钟，等待事务回查
			Thread.sleep(5 * 60 * 1000L);
		} catch (MQClientException e) {
			assertThat(e).hasMessageContaining("Transaction Message Send Error");
		}
	}

	@Test
	public void testSendMessage_DelayTimeLevel() throws RemotingException, InterruptedException, MQBrokerException {
		try {
			Message message = new Message("TransactionTopic", "transactionTest", "msg-1", ("HelloDelayTime").getBytes());
			message.setDelayTimeLevel(1);
			SendResult result = producer.sendMessageInTransaction(message, "HelloDelayTime");
			System.out.printf("Topic:%s send success, misId is:%s%n", message.getTopic(), result.getMsgId());

			// 挂起5分钟，等待事务回查
			Thread.sleep(5 * 60 * 1000L);
		} catch (MQClientException e) {
			assertThat(e).hasMessageContaining("Transaction Message Send Error");
		}
	}
}

/**
 * 简便起见，直接返回Commit
 */
class DelayTimeLevelTransactionListener implements TransactionListener {
	/**
	 * 执行本地事务
	 *
	 * @param message
	 * @param o
	 * @return
	 */
	@Override
	public LocalTransactionState executeLocalTransaction(Message message, Object o) {
		return LocalTransactionState.COMMIT_MESSAGE;
	}

	/**
	 * 回查本地事务结果
	 *
	 * @param messageExt
	 * @return
	 */
	@Override
	public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
		return LocalTransactionState.COMMIT_MESSAGE;
	}
}
