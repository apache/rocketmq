package com.alibaba.rocketmq.example.producer;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

public class OneWayProducer {
	/**
	 * @param args
	 * @throws MQClientException 
	 */
	public static void main(String[] args) throws MQClientException {
		MQProducer synproducer = new DefaultMQProducer("example.producer");

		synproducer.start();

        String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };

        for (int i = 0; i < 10; i++) {
			try {
				Message msg =
	                    new Message("TopicTest", tags[i % tags.length], "KEY" + i, ("Hello RocketMQ from OneWay" + i).getBytes());
				synproducer.sendOneway(msg);
			} catch (RemotingException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (MQClientException e1) {
				e1.printStackTrace();
			}
        }
        synproducer.shutdown();
        System.exit(0);
	}
}
