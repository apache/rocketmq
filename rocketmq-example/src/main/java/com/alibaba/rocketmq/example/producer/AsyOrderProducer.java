package com.alibaba.rocketmq.example.producer;

import java.util.List;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

public class AsyOrderProducer {
	/**
	 * @param args
	 * @throws MQClientException 
	 */
	public static void main(String[] args) throws MQClientException {
		MQProducer asyproducer = new DefaultMQProducer("example.producer");

		asyproducer.start();

        String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
        final SendCallback sendCallback = new SendCallback(){

			@Override
			public void onSuccess(SendResult sendResult) {
				System.out.println(sendResult);
			}

			@Override
			public void onException(Throwable e) {
				e.printStackTrace();
			}
        	
        };
        MessageQueueSelector selector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                Integer id = (Integer) arg;
                int index = id % mqs.size();
                return mqs.get(index);
            }
        };
        for (int i = 0; i < 10; i++) {
			try {
				int orderId = i % 10;
				Message msg =
	                    new Message("TopicTest", tags[i % tags.length], "KEY" + i, ("Hello RocketMQ from asyproducer " + i).getBytes());
				
				asyproducer.send(msg, selector, orderId, sendCallback);
			} catch (RemotingException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (MQClientException e1) {
				e1.printStackTrace();
			}
        }
        asyproducer.shutdown();
        System.exit(0);
	}
}
