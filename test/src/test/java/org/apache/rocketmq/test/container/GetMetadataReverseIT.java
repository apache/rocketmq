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

package org.apache.rocketmq.test.container;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.container.InnerSalveBrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

@Ignore
public class GetMetadataReverseIT extends ContainerIntegrationTestBase {

    private static DefaultMQProducer producer;

    private static final String CONSUMER_GROUP = GetMetadataReverseIT.class.getSimpleName() + "_Consumer";

    private static final int MESSAGE_COUNT = 32;

    private final static Random random = new Random();

    public GetMetadataReverseIT() throws UnsupportedEncodingException {

    }

    @BeforeClass
    public static void beforeClass() throws Throwable {
        producer = createProducer(PushMultipleReplicasIT.class.getSimpleName() + "_PRODUCER");
        producer.setSendMsgTimeout(15 * 1000);
        producer.start();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        producer.shutdown();
    }

    @Test
    public void testGetMetadataReverse_consumerOffset() throws Exception {
        String topic = GetMetadataReverseIT.class.getSimpleName() + "_consumerOffset" + random.nextInt(65535);
        createTopicTo(master1With3Replicas, topic, 1, 1);
        // Wait topic synchronization
        await().atMost(Duration.ofMinutes(1)).until(() -> {
            InnerSalveBrokerController slaveBroker = brokerContainer2.getSlaveBrokers().iterator().next();
            return slaveBroker.getTopicConfigManager().selectTopicConfig(topic) != null;
        });

        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, Integer.toString(i).getBytes());
            SendResult sendResult = producer.send(msg);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                sendSuccess++;
            }
        }
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);
        System.out.printf("send success%n");

        isolateBroker(master1With3Replicas);
        brokerContainer1.removeBroker(new BrokerIdentity(
            master1With3Replicas.getBrokerConfig().getBrokerClusterName(),
            master1With3Replicas.getBrokerConfig().getBrokerName(),
            master1With3Replicas.getBrokerConfig().getBrokerId()));

        System.out.printf("Remove master%n");

        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUMER_GROUP);
        pushConsumer.subscribe(topic, "*");
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            receivedMsgCount.addAndGet(msgs.size());
            msgs.forEach(x -> System.out.printf(x + "%n"));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();
        await().atMost(Duration.ofMinutes(3)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT);

        await().atMost(Duration.ofMinutes(1)).until(() -> {
            pushConsumer.getDefaultMQPushConsumerImpl().persistConsumerOffset();
            Map<Integer, Long> slaveOffsetTable = null;
            for (InnerSalveBrokerController slave : brokerContainer2.getSlaveBrokers()) {
                if (slave.getBrokerConfig().getBrokerName().equals(master1With3Replicas.getBrokerConfig().getBrokerName())) {
                    slaveOffsetTable = slave.getConsumerOffsetManager().queryOffset(CONSUMER_GROUP, topic);
                }
            }

            if (slaveOffsetTable != null) {
                long totalOffset = 0;
                for (final Long offset : slaveOffsetTable.values()) {
                    totalOffset += offset;
                }

                return totalOffset >= MESSAGE_COUNT;
            }
            return false;
        });

        //Add back master
        master1With3Replicas = brokerContainer1.addBroker(master1With3Replicas.getBrokerConfig(), master1With3Replicas.getMessageStoreConfig());
        master1With3Replicas.start();
        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("Add back master%n");

        awaitUntilSlaveOK();

        await().atMost(Duration.ofMinutes(1)).until(() -> {
            Map<Integer, Long> offsetTable = master1With3Replicas.getConsumerOffsetManager().queryOffset(CONSUMER_GROUP, topic);
            long totalOffset = 0;
            if (offsetTable != null) {
                for (final Long offset : offsetTable.values()) {
                    totalOffset += offset;
                }
            }
            return totalOffset >= MESSAGE_COUNT;
        });

        pushConsumer.shutdown();
    }

    @Test
    public void testGetMetadataReverse_delayOffset() throws Exception {
        String topic = GetMetadataReverseIT.class.getSimpleName() + "_delayOffset" + random.nextInt(65535);
        createTopicTo(master1With3Replicas, topic, 1, 1);
        createTopicTo(master2With3Replicas, topic, 1, 1);
        createTopicTo(master3With3Replicas, topic, 1, 1);
        // Wait topic synchronization
        await().atMost(Duration.ofMinutes(1)).until(() -> {
            InnerSalveBrokerController slaveBroker = brokerContainer2.getSlaveBrokers().iterator().next();
            return slaveBroker.getTopicConfigManager().selectTopicConfig(topic) != null;
        });
        int delayLevel = 4;

        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUMER_GROUP);
        pushConsumer.subscribe(topic, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            receivedMsgCount.addAndGet(msgs.size());
            msgs.forEach(x -> System.out.printf(x + "%n"));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        MessageQueue messageQueue = new MessageQueue(topic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, Integer.toString(i).getBytes());
            msg.setDelayTimeLevel(delayLevel);
            SendResult sendResult = producer.send(msg, messageQueue);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                sendSuccess++;
            }
        }
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);
        System.out.printf("send success%n");

        isolateBroker(master1With3Replicas);
        brokerContainer1.removeBroker(new BrokerIdentity(
            master1With3Replicas.getBrokerConfig().getBrokerClusterName(),
            master1With3Replicas.getBrokerConfig().getBrokerName(),
            master1With3Replicas.getBrokerConfig().getBrokerId()));

        System.out.printf("Remove master%n");

        await().atMost(Duration.ofMinutes(1)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT);

        await().atMost(Duration.ofMinutes(1)).until(() -> {
            pushConsumer.getDefaultMQPushConsumerImpl().persistConsumerOffset();
            Map<Integer, Long> OffsetTable = master2With3Replicas.getConsumerOffsetManager().queryOffset(CONSUMER_GROUP, topic);
            if (OffsetTable != null) {
                long totalOffset = 0;
                for (final Long offset : OffsetTable.values()) {
                    totalOffset += offset;
                }
                return totalOffset >= MESSAGE_COUNT;

            }
            return false;
        });

        //Add back master
        master1With3Replicas = brokerContainer1.addBroker(master1With3Replicas.getBrokerConfig(), master1With3Replicas.getMessageStoreConfig());
        master1With3Replicas.start();
        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("Add back master%n");

        awaitUntilSlaveOK();

        await().atMost(Duration.ofMinutes(1)).until(() -> {
            Map<Integer, Long> offsetTable = master1With3Replicas.getScheduleMessageService().getOffsetTable();
            System.out.println("" + offsetTable.get(delayLevel));
            return offsetTable.get(delayLevel) >= MESSAGE_COUNT;
        });

        pushConsumer.shutdown();
    }

    @Test
    public void testGetMetadataReverse_timerCheckPoint() throws Exception {
        String topic = GetMetadataReverseIT.class.getSimpleName() + "_timerCheckPoint" + random.nextInt(65535);
        createTopicTo(master1With3Replicas, topic, 1, 1);
        createTopicTo(master2With3Replicas, topic, 1, 1);
        createTopicTo(master3With3Replicas, topic, 1, 1);
        // Wait topic synchronization
        await().atMost(Duration.ofMinutes(1)).until(() -> {
            InnerSalveBrokerController slaveBroker = brokerContainer2.getSlaveBrokers().iterator().next();
            return slaveBroker.getTopicConfigManager().selectTopicConfig(topic) != null;
        });

        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUMER_GROUP);
        pushConsumer.subscribe(topic, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            receivedMsgCount.addAndGet(msgs.size());
            msgs.forEach(x -> System.out.printf(x + "%n"));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        MessageQueue messageQueue = new MessageQueue(topic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, Integer.toString(i).getBytes());
            msg.setDelayTimeSec(30);
            SendResult sendResult = producer.send(msg, messageQueue);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                sendSuccess++;
            }
        }
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);
        System.out.printf("send success%n");

        isolateBroker(master1With3Replicas);
        brokerContainer1.removeBroker(new BrokerIdentity(
            master1With3Replicas.getBrokerConfig().getBrokerClusterName(),
            master1With3Replicas.getBrokerConfig().getBrokerName(),
            master1With3Replicas.getBrokerConfig().getBrokerId()));

        System.out.printf("Remove master%n");

        await().atMost(Duration.ofMinutes(1)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT);

        await().atMost(Duration.ofMinutes(1)).until(() -> {
            pushConsumer.getDefaultMQPushConsumerImpl().persistConsumerOffset();
            Map<Integer, Long> OffsetTable = master2With3Replicas.getConsumerOffsetManager().queryOffset(CONSUMER_GROUP, topic);
            if (OffsetTable != null) {
                long totalOffset = 0;
                for (final Long offset : OffsetTable.values()) {
                    totalOffset += offset;
                }
                return totalOffset >= MESSAGE_COUNT;
            }
            return false;
        });

        //Add back master
        master1With3Replicas = brokerContainer1.addBroker(master1With3Replicas.getBrokerConfig(), master1With3Replicas.getMessageStoreConfig());
        master1With3Replicas.start();
        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("Add back master%n");

        awaitUntilSlaveOK();

        await().atMost(Duration.ofMinutes(1)).until(() -> master1With3Replicas.getTimerCheckpoint().getMasterTimerQueueOffset() >= MESSAGE_COUNT);

        pushConsumer.shutdown();
    }
}
