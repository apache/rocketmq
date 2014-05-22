package com.alibaba.rocketmq.test.integration;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * description
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-3-19
 */
public class UnitOrderTest {
    private static String[] topics = new String[] { "qatest_TopicTest_Order" };
    private static String[] nsAddrs = new String[] { "10.232.26.122:9876" };
    // private static String[] topics = new String[] { "qatest_TopicTest_Order",
    // "qatest_TopicTest_Order2",
    // "qatest_TopicTest_Order3" };
    // private static String[] nsAddrs = new String[] { "10.232.26.122:9876",
    // "10.232.25.81:9876" };
    private static MQPushConsumer[] consumers = new MQPushConsumer[nsAddrs.length];
    private static String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
    private static String consumerGroup = "qatest_consumer_order";
    private static String producerGroup = "qatest_producer_order";

    private static long start = System.currentTimeMillis();
    private static boolean isSuspend = false;


    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 同步到多个单元
        for (int i = 0; i < nsAddrs.length; i++) {
            consumers[i] = createConsumer(nsAddrs[i], topics);
        }

        Runnable runnable = new Runnable() {
            public void run() {
                while (true) {
                    if (!isSuspend && System.currentTimeMillis() - start > 10 * 1000) {
	                    isSuspend = true;
	                    System.out.println("suspend start.....");
	                    consumers[0].suspend();
	                    start = System.currentTimeMillis();
                    }
                    else if (isSuspend && System.currentTimeMillis() - start > 10 * 1000) {
	                    isSuspend = false;
	                    System.out.println("resume start.....");
	                    consumers[0].resume();
	                    start = System.currentTimeMillis();
                    }
                }
            }
        };

        Thread t = new Thread(runnable);
        t.start();

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
        int orderId = 0;
        while (true) {
            for (String topic : topics) {
                try {
                    Message msg =
                            new Message(topic, tags[orderId % tags.length], "KEY" + orderId,
                                ("hello jodie#" + orderId).getBytes());
                    SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Integer id = (Integer) arg;
                            int index = id % mqs.size();
                            return mqs.get(index);
                        }
                    }, orderId);
                    // System.out.println("sendStatus=" +
                    // sendResult.getSendStatus() + ", topic="
                    // + msg.getTopic() + ", body=" + new
                    // String(msg.getBody()));
                }
                catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(500);
                }
            }
            orderId++;
        }
    }


    private static MQPushConsumer createConsumer(String nsAddr, String... topics) throws MQClientException {
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
        consumer.setConsumeThreadMax(1);
        consumer.setConsumeThreadMin(1);

        consumer.start();
        return consumer;
    }


    private static MessageListener createMessageListener() {
        return new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.err.println("topic:" + msg.getTopic() + ", msgId:" + msg.getMsgId() + ", body:"
                            + new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        };
    }
}
