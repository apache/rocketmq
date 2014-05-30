package com.alibaba.rocketmq.test.integration;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.*;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;


/**
 * description
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-3-19
 */
public class UnitCheckTest {
    private static String[] topics = new String[] { "qatest_TopicTest", "qatest_TopicTest2",
                                                   "qatest_TopicTest3_virtual_order",
                                                   "qatest_TopicTest3_virtual_order2" };
    private static String[] orderTopics = new String[] { "qatest_TopicTest_Order" };
    private static String nsAddr = "10.232.25.83:9876";
//    private static String nsAddr = "10.232.26.122:9876";
    private static String consumerGroup = "unit_consumer";
    private static String orderConsumerGroup = "order_unit_consumer";
    private static DaoService dao = new DaoService();


    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 同步到多个单元
        createConsumer(nsAddr, topics);
        createOrderConsumer(nsAddr, orderTopics);
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
                    String body = new String(msg.getBody());
                    System.err.println("topic:" + msg.getTopic() + ", body:" + body);
                    dao.delete(body);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        };
    }


    private static MQPushConsumer createOrderConsumer(String nsAddr, String... topics)
            throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(orderConsumerGroup);
        consumer.setInstanceName("unit_consumer_" + System.nanoTime());
        consumer.setNamesrvAddr(nsAddr);
        for (String topic : topics) {
            consumer.subscribe(topic, "*");
        }

        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        MessageListener listener = createOrderMessageListener();
        consumer.registerMessageListener(listener);
        consumer.setConsumeThreadMax(1);
        consumer.setConsumeThreadMin(1);

        consumer.start();
        return consumer;
    }


    private static MessageListener createOrderMessageListener() {
        return new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    String body = new String(msg.getBody());
                    System.err.println("topic:" + msg.getTopic() + ", body:" + body);
                    dao.delete(body);
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        };
    }
}
