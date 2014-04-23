package com.alibaba.rocketmq.test.integration;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * description
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-3-19
 */
public class UnitNormalTest {
    private static String[] topics = new String[] { "qatest_TopicTest", "qatest_TopicTest2",
                                                   "qatest_TopicTest3_virtual_order",
                                                   "qatest_TopicTest3_virtual_order2" };
    private static String[] nsAddrs = new String[] { "10.232.26.122:9876", "10.232.25.81:9876" };
    private static String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
    private static String consumerGroup = "qatest_consumer";
    private static String producerGroup = "qatest_producer";


    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 同步到多个单元
        for (String nsAddr : nsAddrs) {
            createConsumer(nsAddr, topics);
        }

        // 只发一个单元
        DefaultMQProducer producer0 = crateProducer(producerGroup, nsAddrs[0]);
        producer0.start();
        sendMsg(producer0, topics);
    }


    private static DefaultMQProducer crateProducer(String group, String nsAddr) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setInstanceName("unit_producer_" + System.nanoTime());
        producer.setNamesrvAddr(nsAddr);
        return producer;
    }


    public static void sendMsg(DefaultMQProducer producer, String... topics) throws MQClientException,
            InterruptedException {
        int i = 0;
        while (true) {
            for (String topic : topics) {
                try {
                    Message msg =
                            new Message(topic, tags[i % tags.length], "KEY" + i,
                                ("hello jodie#" + i).getBytes());
                    SendResult sendResult = producer.send(msg);
                    System.out.println("sendStatus=" + sendResult.getSendStatus() + ", topic="
                            + msg.getTopic() + ", body=" + new String(msg.getBody()));
                    i++;
                }
                catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(500);
                }
            }
        }
    }


    private static void createConsumer(String nsAddr, String... topics) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setInstanceName("unit_consumer_" + System.nanoTime());
        consumer.setNamesrvAddr(nsAddr);
        for (String topic : topics) {
            consumer.subscribe(topic, "*");
        }

        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        MessageListener listener = createMessageListener();
        consumer.registerMessageListener(listener);

        consumer.start();
    }


    private static MessageListener createMessageListener() {
        return new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.err.println("topic:" + msg.getTopic() + ", msgId:" + msg.getMsgId() + ", body:"
                            + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        };
    }
}
