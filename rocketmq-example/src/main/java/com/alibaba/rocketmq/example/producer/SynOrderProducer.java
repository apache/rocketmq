package com.alibaba.rocketmq.example.producer;

import java.util.List;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


public class SynOrderProducer {
    /**
     * @param args
     * @throws MQClientException
     */
    public static void main(String[] args) throws MQClientException {

        MQProducer synOrderproducer = new DefaultMQProducer("example.producer");

        synOrderproducer.start();

        String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
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
                        new Message("TopicTest", tags[i % tags.length], "KEY" + i,
                            ("Hello RocketMQ from synproducer" + i).getBytes());
                SendResult sendResult;

                sendResult = synOrderproducer.send(msg, selector, orderId);
                System.out.println(sendResult);
            }
            catch (RemotingException e) {
                e.printStackTrace();
            }
            catch (MQBrokerException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (MQClientException e1) {
                e1.printStackTrace();
            }
        }
        synOrderproducer.shutdown();
        System.exit(0);
    }
}
