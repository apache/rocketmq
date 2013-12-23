package com.alibaba.rocketmq.test.integration.normal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class PushConsumerTest extends NormalBaseTest {
    private DefaultMQPushConsumer consumer;


    @Before
    public void before() throws Exception {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setInstanceName(instanceName + System.nanoTime());
        consumer.subscribe(topic, getTagsExpression(0));
        // consumer.subscribe(topic, getTagsExpression(3));
    }


    @Test
    public void pushConsumerFromLastOffset() throws MQClientException {
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        MessageListener listener = createMessageListener();
        consumer.registerMessageListener(listener);

        consumer.start();
        System.out.println("Consumer Started.");
        try {
            System.in.read();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void pushConsumerFromMinOffset() throws MQClientException {
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        MessageListener listener = createMessageListener();
        consumer.registerMessageListener(listener);

        consumer.start();
        System.out.println("Consumer Started.");
        try {
            System.in.read();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void pushConsumerByBroadCast() throws MQClientException {
        consumer.setMessageModel(MessageModel.BROADCASTING);
        MessageListener listener = createMessageListener();
        consumer.registerMessageListener(listener);

        consumer.start();
        System.out.println("Consumer Started.");
        try {
            System.in.read();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    private MessageListener createMessageListener() {
        return new MessageListenerConcurrently() {
            AtomicLong consumeTimes = new AtomicLong(0);


            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {

                this.consumeTimes.incrementAndGet();
                if ((this.consumeTimes.get() % 11) == 0) {
                    System.out.println("Delay 0========Receive New Messages: " + msgs);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                else if ((this.consumeTimes.get() % 22) == 0) {
                    System.out.println(Thread.currentThread().getName()
                            + "Delay 5========Receive New Messages: " + msgs);
                    context.setDelayLevelWhenNextConsume(5);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                System.out.println("No Delay========Receive New Messages: " + msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        };
    }
}
