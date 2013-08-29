package com.alibaba.rocketmq.test.integration.order;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class ProducerTest extends OrderBaseTest {
    private DefaultMQProducer producer;


    @Before
    public void before() {
        producer = new DefaultMQProducer(producerGroup);
        producer.setInstanceName(instanceName);
    }


    @Test
    public void producerTest() throws MQClientException, RemotingException, InterruptedException,
            MQBrokerException {
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 订单ID相同的消息要有序
            // int orderId = i % 10;
            int orderId = 0;
            Message msg =
                    new Message(topic, tags[i % tags.length], "KEY" + i, ("Hello RocketMQ " + i).getBytes());
            SendResult result = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);
            System.out.println(result);
        }
        producer.shutdown();
    }
}
