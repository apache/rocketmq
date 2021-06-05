/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeStagedConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.MessageListenerStagedConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConsumeMessageStagedConcurrentlyServiceTest {
    private String consumerGroup;
    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private DefaultMQPushConsumer pushConsumer;

    @Before
    public void init() throws Exception {
        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
    }

    @Test
    public void test01ConsumeMessageDirectlyWithNoException() {
        Map<ConsumeOrderlyStatus, CMResult> map = new HashMap<>();
        map.put(ConsumeOrderlyStatus.SUCCESS, CMResult.CR_SUCCESS);
        map.put(ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT, CMResult.CR_LATER);
        map.put(ConsumeOrderlyStatus.COMMIT, CMResult.CR_COMMIT);
        map.put(ConsumeOrderlyStatus.ROLLBACK, CMResult.CR_ROLLBACK);
        map.put(null, CMResult.CR_RETURN_NULL);

        for (ConsumeOrderlyStatus consumeOrderlyStatus : map.keySet()) {
            final ConsumeOrderlyStatus status = consumeOrderlyStatus;
            MessageListenerStagedConcurrently stagedConcurrently = new MessageListenerStagedConcurrently() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,
                    final ConsumeStagedConcurrentlyContext context) {
                    return status;
                }
            };

            ConsumeMessageStagedConcurrentlyService stagedConcurrentlyService = new ConsumeMessageStagedConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), stagedConcurrently);
            MessageExt msg = new MessageExt();
            msg.setTopic(topic);
            Assert.assertEquals(stagedConcurrentlyService.consumeMessageDirectly(msg, brokerName).getConsumeResult(), map.get(consumeOrderlyStatus));
        }

    }

    @Test
    public void test02ConsumeMessageDirectlyWithException() {
        MessageListenerStagedConcurrently stagedConcurrently = new MessageListenerStagedConcurrently() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,
                final ConsumeStagedConcurrentlyContext context) {
                throw new RuntimeException();
            }
        };

        ConsumeMessageStagedConcurrentlyService stagedConcurrentlyService = new ConsumeMessageStagedConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), stagedConcurrently);
        MessageExt msg = new MessageExt();
        msg.setTopic(topic);
        Assert.assertEquals(stagedConcurrentlyService.consumeMessageDirectly(msg, brokerName).getConsumeResult(), CMResult.CR_THROW_EXCEPTION);
    }

    //@Test
    public void test03EvolveIntoMessageListenerOrderly() throws Throwable {
        DefaultMQProducer producer = new DefaultMQProducer(consumerGroup);
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        System.out.println("producer started !");

        for (int i = 0; i < 100; i++) {
            Message message = new Message(topic + "2",
                "ssss2",
                ("AsyncProducer2 say " + i).getBytes());

            SendResult result = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, i);
            System.out.println(result);
        }
        producer.shutdown();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        int pullBatchSize = consumer.getPullBatchSize();
        int poolSize = 4 * pullBatchSize;
        consumer.setConsumeThreadMin(poolSize);
        consumer.setConsumeThreadMax(poolSize);
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(topic + "2", "ssss2");
        consumer.registerMessageListener(new MessageListenerStagedConcurrently() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeStagedConcurrentlyContext context) {
                try {
                    Thread.sleep(new Random().nextInt(20));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                MessageExt messageExt = msgs.get(0);
                System.out.println(context.getStageIndex() + " " + messageExt.getQueueId() + " " + messageExt.getMsgId() + " " + new String(messageExt.getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }

            @Override
            public List<Integer> getStageDefinitions() {
                List<Integer> list = new ArrayList<>();
                for (int i = 0; i < 10000; i++) {
                    list.add(1);
                }
                return list;
            }
        });
        consumer.start();
        //please change to a larger millis when running local
        Thread.sleep(10000);
        consumer.shutdown();
    }

    //@Test
    public void test04DegenerateIntoMessageListenerConcurrently() throws Throwable {
        DefaultMQProducer producer = new DefaultMQProducer(consumerGroup);
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        System.out.println("producer started !");

        for (int i = 0; i < 100; i++) {
            Message message = new Message(topic + "1",
                "ssss1",
                ("AsyncProducer1 say " + i).getBytes());

            SendResult result = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, i);
            System.out.println(result);
        }
        producer.shutdown();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        int pullBatchSize = consumer.getPullBatchSize();
        int poolSize = 4 * pullBatchSize;
        consumer.setConsumeThreadMin(poolSize);
        consumer.setConsumeThreadMax(poolSize);
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(topic + "1", "ssss1");
        consumer.registerMessageListener(new MessageListenerStagedConcurrently() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeStagedConcurrentlyContext context) {
                try {
                    Thread.sleep(new Random().nextInt(20));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                MessageExt messageExt = msgs.get(0);
                System.out.println(context.getStageIndex() + " " + messageExt.getQueueId() + " " + messageExt.getMsgId() + " " + new String(messageExt.getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        //please change to a larger millis when running local
        Thread.sleep(10000);
        consumer.shutdown();
    }

    //@Test
    public void test05MessageListenerOrderlyToConcurrently() throws Throwable {
        DefaultMQProducer producer = new DefaultMQProducer(consumerGroup);
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        System.out.println("producer started !");

        for (int i = 0; i < 100; i++) {
            Message message = new Message(topic + "3",
                "ssss3",
                ("AsyncProducer1 say " + i).getBytes());

            SendResult result = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, i);
            System.out.println(result);
        }
        producer.shutdown();

        System.setProperty("rocketmq.client.rebalance.lockMaxLiveTime", "3000");
        System.setProperty("rocketmq.client.rebalance.lockInterval", "2000");
        System.setProperty("rocketmq.client.pull.pullMaxIdleTime", "12000");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        int pullBatchSize = consumer.getPullBatchSize();
        int poolSize = 4 * pullBatchSize;
        consumer.setConsumeThreadMin(poolSize);
        consumer.setConsumeThreadMax(poolSize);
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(topic + "3", "ssss3");
        consumer.registerMessageListener(new MessageListenerStagedConcurrently() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeStagedConcurrentlyContext context) {
                try {
                    Thread.sleep(new Random().nextInt(20));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                MessageExt messageExt = msgs.get(0);
                System.out.println(context.getStageIndex() + " " + messageExt.getQueueId() + " " + messageExt.getMsgId() + " " + new String(messageExt.getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }

            /**
             * After consuming 1+2+3+4+5+6+7+8+9+10=55 messages,
             * it has completely evolved from orderly consumer
             * to concurrently consumer.
             */
            @Override
            public List<Integer> getStageDefinitions() {
                List<Integer> list = new ArrayList<>();
                for (int i = 1; i <= 10; i++) {
                    list.add(i);
                }
                return list;
            }
        });
        consumer.start();
        //please change to a larger millis when running local
        Thread.sleep(10000);
        consumer.shutdown();
    }
}
