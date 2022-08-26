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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.container.BrokerContainer;
import org.apache.rocketmq.container.InnerBrokerController;
import org.apache.rocketmq.container.InnerSalveBrokerController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Ignore
public class PopSlaveActingMasterIT extends ContainerIntegrationTestBase {
    private static final String CONSUME_GROUP = PopSlaveActingMasterIT.class.getSimpleName() + "_Consumer";
    private final static int MESSAGE_COUNT = 16;
    private final static Random random = new Random();
    private static DefaultMQProducer producer;
    private final static String MESSAGE_STRING = RandomStringUtils.random(1024);
    private static byte[] MESSAGE_BODY;

    public PopSlaveActingMasterIT() {
    }

    static {
        try {
            MESSAGE_BODY = MESSAGE_STRING.getBytes(RemotingHelper.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    void createTopic(String topic) {
        createTopicTo(master1With3Replicas, topic, 1, 1);
        createTopicTo(master2With3Replicas, topic, 1, 1);
        createTopicTo(master3With3Replicas, topic, 1, 1);
        System.out.println("Topic [" + topic + "] created");
    }

    @BeforeClass
    public static void beforeClass() throws Throwable {
        producer = createProducer(PopSlaveActingMasterIT.class.getSimpleName() + "_PRODUCER");
        producer.setSendMsgTimeout(5000);
        producer.start();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        producer.shutdown();
    }


    @Test
    public void testLocalActing_ackSlave() throws Exception {
        String topic = PopSlaveActingMasterIT.class.getSimpleName() + random.nextInt(65535);
        createTopic(topic);
        String retryTopic = KeyBuilder.buildPopRetryTopic(topic, CONSUME_GROUP);
        createTopic(retryTopic);

        this.switchPop(topic);

        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);

        MessageQueue messageQueue = new MessageQueue(topic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, MESSAGE_BODY);
            SendResult sendResult = producer.send(msg, messageQueue);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                System.out.println("send message id: " + sendResult.getMsgId());
                sendSuccess++;
            }
        }

        System.out.printf("send success %d%n", sendSuccess);
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);

        isolateBroker(master1With3Replicas);
        System.out.printf("isolate master1%n");

        DefaultMQPushConsumer consumer = createPushConsumer(CONSUME_GROUP);
        consumer.subscribe(topic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        List<String> consumedMessages = new CopyOnWriteArrayList<>();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                System.out.println("receive msg id: " + msg.getMsgId());
                consumedMessages.add(msg.getMsgId());
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.setClientRebalance(false);
        consumer.start();

        await().atMost(Duration.ofMinutes(1)).until(() -> consumedMessages.size() >= MESSAGE_COUNT);
        System.out.printf("%s pop receive msg count: %d%n", LocalDateTime.now(), consumedMessages.size());

        consumer.shutdown();

        List<String> retryMsgList = new CopyOnWriteArrayList<>();
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(retryTopic, "*");
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("receive retry msg: %s %s%n", new String(msg.getBody()), msg);
                retryMsgList.add(new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        System.out.printf("wait for ack revive%n");
        Thread.sleep(10000L);

        assertThat(retryMsgList.size()).isEqualTo(0);

        cancelIsolatedBroker(master1With3Replicas);
        awaitUntilSlaveOK();

        pushConsumer.shutdown();
    }

    @Test
    public void testLocalActing_notAckSlave() throws Exception {
        String topic = PopSlaveActingMasterIT.class.getSimpleName() + random.nextInt(65535);
        createTopic(topic);
        String retryTopic = KeyBuilder.buildPopRetryTopic(topic, CONSUME_GROUP);
        createTopic(retryTopic);

        this.switchPop(topic);

        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);

        Set<String> sendToIsolateMsgSet = new HashSet<>();
        MessageQueue messageQueue = new MessageQueue(topic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, MESSAGE_BODY);
            SendResult sendResult = producer.send(msg, messageQueue);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                sendToIsolateMsgSet.add(new String(msg.getBody()));
                sendSuccess++;
            }
        }

        System.out.printf("send success %d%n", sendSuccess);
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);

        isolateBroker(master1With3Replicas);
        System.out.printf("isolate master1%n");

        DefaultMQPushConsumer consumer = createPushConsumer(CONSUME_GROUP);
        consumer.subscribe(topic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setPopInvisibleTime(5000L);
        List<String> consumedMessages = new CopyOnWriteArrayList<>();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                System.out.println("receive msg id: " + msg.getMsgId());

                msg.setReconsumeTimes(0);

                consumedMessages.add(msg.getMsgId());
            });
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });
        consumer.setClientRebalance(false);
        consumer.start();

        await().atMost(Duration.ofMinutes(1)).until(() -> consumedMessages.size() >= MESSAGE_COUNT);
        consumer.shutdown();

        List<String> retryMsgList = new CopyOnWriteArrayList<>();
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(retryTopic, "*");
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("receive retry msg: %s%n", msg.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                retryMsgList.add(new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        System.out.printf(LocalDateTime.now() + ": wait for ack revive%n");

        AtomicInteger failCnt = new AtomicInteger(0);
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(10)).until(() -> {
            if (retryMsgList.size() < MESSAGE_COUNT) {
                System.out.println("check FAILED" + failCnt.incrementAndGet() + ": retryMsgList.size=" + retryMsgList.size() + " less than " + MESSAGE_COUNT);
                return false;
            }

            for (String msgBodyString : retryMsgList) {
                if (!sendToIsolateMsgSet.contains(msgBodyString)) {
                    System.out.println("check FAILED: sendToIsolateMsgSet doesn't contain " + msgBodyString);
                    return false;
                }
            }
            return true;
        });

        System.out.printf(LocalDateTime.now() + ": receive retry msg size=%d%n", retryMsgList.size());

        cancelIsolatedBroker(master1With3Replicas);
        awaitUntilSlaveOK();

        pushConsumer.shutdown();
    }

    @Test
    public void testRemoteActing_ackSlave() throws Exception {
        String topic = PopSlaveActingMasterIT.class.getSimpleName() + random.nextInt(65535);
        createTopic(topic);
        String retryTopic = KeyBuilder.buildPopRetryTopic(topic, CONSUME_GROUP);
        createTopic(retryTopic);

        switchPop(topic);

        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);

        MessageQueue messageQueue = new MessageQueue(topic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, MESSAGE_BODY);
            SendResult sendResult = producer.send(msg, messageQueue);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                System.out.println("Send message id: " + sendResult.getMsgId());
                sendSuccess++;
            }
        }

        System.out.printf("%s send success %d%n", LocalDateTime.now(), sendSuccess);
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);

        isolateBroker(master1With3Replicas);
        System.out.printf("%s isolate master1%n", LocalDateTime.now());

        isolateBroker(master2With3Replicas);
        brokerContainer2.removeBroker(new BrokerIdentity(
                master2With3Replicas.getBrokerConfig().getBrokerClusterName(),
                master2With3Replicas.getBrokerConfig().getBrokerName(),
                master2With3Replicas.getBrokerConfig().getBrokerId()));
        System.out.printf("%s Remove master2%n", LocalDateTime.now());


        DefaultMQPushConsumer consumer = createPushConsumer(CONSUME_GROUP);
        consumer.subscribe(topic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        List<String> consumedMessages = new CopyOnWriteArrayList<>();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                System.out.println("receive msg id: " + msg.getMsgId());
                consumedMessages.add(msg.getMsgId());
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.setClientRebalance(false);
        consumer.start();

        await().atMost(Duration.ofMinutes(2)).until(() -> consumedMessages.size() >= MESSAGE_COUNT);
        consumer.shutdown();
        System.out.printf("%s %d messages consumed%n", LocalDateTime.now(), consumedMessages.size());

        List<String> retryMsgList = new CopyOnWriteArrayList<>();
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(retryTopic, "*");
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("receive retry msg: %s %n", msg.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                retryMsgList.add(new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        System.out.printf("%s wait for ack revive%n", LocalDateTime.now());
        Thread.sleep(10000);

        assertThat(retryMsgList.size()).isEqualTo(0);

        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("%s Cancel isolate master1%n", LocalDateTime.now());

        //Add back master
        master2With3Replicas = brokerContainer2.addBroker(master2With3Replicas.getBrokerConfig(), master2With3Replicas.getMessageStoreConfig());
        master2With3Replicas.start();
        cancelIsolatedBroker(master2With3Replicas);
        System.out.printf("%s Add back master2%n", LocalDateTime.now());

        awaitUntilSlaveOK();

        System.out.printf("%s wait for ack revive%n", LocalDateTime.now());
        Thread.sleep(10000);

        assertThat(retryMsgList.size()).isEqualTo(0);

        System.out.printf("%s shutting down pushConsumer%n", LocalDateTime.now());
        pushConsumer.shutdown();
    }

    @Test
    public void testRemoteActing_notAckSlave_getFromLocal() throws Exception {
        String topic = PopSlaveActingMasterIT.class.getSimpleName() + random.nextInt(65535);
        createTopic(topic);
        this.switchPop(topic);

        String retryTopic = KeyBuilder.buildPopRetryTopic(topic, CONSUME_GROUP);
        createTopic(retryTopic);

        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);

        Set<String> sendToIsolateMsgSet = new HashSet<>();
        MessageQueue messageQueue = new MessageQueue(topic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, MESSAGE_BODY);
            SendResult sendResult = producer.send(msg, messageQueue);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                sendToIsolateMsgSet.add(new String(msg.getBody()));
                sendSuccess++;
            }
        }

        System.out.printf("send success %d%n", sendSuccess);
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);

        isolateBroker(master1With3Replicas);
        System.out.printf("isolate master1%n");

        isolateBroker(master2With3Replicas);
        brokerContainer2.removeBroker(new BrokerIdentity(
                master2With3Replicas.getBrokerConfig().getBrokerClusterName(),
                master2With3Replicas.getBrokerConfig().getBrokerName(),
                master2With3Replicas.getBrokerConfig().getBrokerId()));
        System.out.printf("Remove master2%n");


        DefaultMQPushConsumer consumer = createPushConsumer(CONSUME_GROUP);
        consumer.subscribe(topic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        List<String> consumedMessages = new CopyOnWriteArrayList<>();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                System.out.println("receive msg id: " + msg.getMsgId());
                consumedMessages.add(msg.getMsgId());
            });
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });
        consumer.setClientRebalance(false);
        consumer.start();

        await().atMost(Duration.ofMinutes(3)).until(() -> consumedMessages.size() >= MESSAGE_COUNT);
        consumer.shutdown();


        List<String> retryMsgList = new CopyOnWriteArrayList<>();
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(retryTopic, "*");
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("receive retry msg: %s%n", msg.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                retryMsgList.add(new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        System.out.printf("wait for ack revive%n");

        await().atMost(Duration.ofMinutes(1)).until(() -> {
            if (retryMsgList.size() < MESSAGE_COUNT) {
                System.out.println("check FAILED: retryMsgList.size=" + retryMsgList.size() + " less than " + MESSAGE_COUNT);
                return false;
            }

            for (String msgBodyString : retryMsgList) {
                if (!sendToIsolateMsgSet.contains(msgBodyString)) {
                    System.out.println("check FAILED: sendToIsolateMsgSet doesn't contain: " + msgBodyString);
                    return false;
                }
            }
            return true;
        });

        System.out.printf("receive retry msg as expected%n");

        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("Cancel isolate master1%n");

        //Add back master
        master2With3Replicas = brokerContainer2.addBroker(master2With3Replicas.getBrokerConfig(), master2With3Replicas.getMessageStoreConfig());
        master2With3Replicas.start();
        cancelIsolatedBroker(master2With3Replicas);
        System.out.printf("Add back master2%n");

        awaitUntilSlaveOK();
        pushConsumer.shutdown();
    }

    @Test
    public void testRemoteActing_notAckSlave_getFromRemote() throws Exception {
        String topic = PopSlaveActingMasterIT.class.getSimpleName() + random.nextInt(65535);
        createTopic(topic);
        this.switchPop(topic);
        String retryTopic = KeyBuilder.buildPopRetryTopic(topic, CONSUME_GROUP);
        createTopic(retryTopic);

        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);

        Set<String> sendToIsolateMsgSet = new HashSet<>();
        MessageQueue messageQueue = new MessageQueue(topic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, MESSAGE_BODY);
            SendResult sendResult = producer.send(msg, messageQueue);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                sendToIsolateMsgSet.add(new String(msg.getBody()));
                sendSuccess++;
            }
        }

        System.out.printf("send success %d%n", sendSuccess);
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);

        isolateBroker(master1With3Replicas);
        System.out.printf("isolate master1%n");

        isolateBroker(master2With3Replicas);
        brokerContainer2.removeBroker(new BrokerIdentity(
                master2With3Replicas.getBrokerConfig().getBrokerClusterName(),
                master2With3Replicas.getBrokerConfig().getBrokerName(),
                master2With3Replicas.getBrokerConfig().getBrokerId()));
        System.out.printf("Remove master2%n");

        BrokerController slave1InBrokerContainer3 = getSlaveFromContainerByName(brokerContainer3, master1With3Replicas.getBrokerConfig().getBrokerName());
        isolateBroker(slave1InBrokerContainer3);
        brokerContainer3.removeBroker(new BrokerIdentity(
                slave1InBrokerContainer3.getBrokerConfig().getBrokerClusterName(),
                slave1InBrokerContainer3.getBrokerConfig().getBrokerName(),
                slave1InBrokerContainer3.getBrokerConfig().getBrokerId()));
        System.out.printf("Remove slave1 form container3%n");

        DefaultMQPushConsumer consumer = createPushConsumer(CONSUME_GROUP);
        consumer.subscribe(topic, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        List<String> consumedMessages = new CopyOnWriteArrayList<>();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                System.out.println("receive msg id: " + msg.getMsgId());
                consumedMessages.add(msg.getMsgId());
            });
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });
        consumer.setClientRebalance(false);
        consumer.start();

        await().atMost(Duration.ofMinutes(1)).until(() -> consumedMessages.size() >= MESSAGE_COUNT);
        System.out.printf("%s pop receive msg count: %d%n", LocalDateTime.now(), consumedMessages.size());
        consumer.shutdown();


        List<String> retryMsgList = new CopyOnWriteArrayList<>();
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(retryTopic, "*");
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("receive retry msg: %s%n", msg.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                retryMsgList.add(new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        System.out.printf("wait for ack revive%n");
        Thread.sleep(10000);

        await().atMost(Duration.ofMinutes(1)).until(() -> {
            if (retryMsgList.size() < MESSAGE_COUNT) {
                return false;
            }

            for (String msgBodyString : retryMsgList) {
                if (!sendToIsolateMsgSet.contains(msgBodyString)) {
                    return false;
                }
            }
            return true;
        });

        System.out.printf("receive retry msg as expected%n");

        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("Cancel isolate master1%n");

        //Add back master
        master2With3Replicas = brokerContainer2.addBroker(master2With3Replicas.getBrokerConfig(), master2With3Replicas.getMessageStoreConfig());
        master2With3Replicas.start();
        cancelIsolatedBroker(master2With3Replicas);
        System.out.printf("Add back master2%n");

        //Add back slave1 to container3
        slave1InBrokerContainer3 = brokerContainer3.addBroker(slave1InBrokerContainer3.getBrokerConfig(), slave1InBrokerContainer3.getMessageStoreConfig());
        slave1InBrokerContainer3.start();
        cancelIsolatedBroker(slave1InBrokerContainer3);
        System.out.printf("Add back slave1 to container3%n");

        awaitUntilSlaveOK();
        pushConsumer.shutdown();
    }

    private void switchPop(String topic) throws Exception {
        for (BrokerContainer brokerContainer : brokerContainerList) {
            for (InnerBrokerController master : brokerContainer.getMasterBrokers()) {
                String brokerAddr = master.getBrokerAddr();
                defaultMQAdminExt.setMessageRequestMode(brokerAddr, topic, CONSUME_GROUP, MessageRequestMode.POP, 8, 60_000);
            }
            for (InnerSalveBrokerController slave : brokerContainer.getSlaveBrokers()) {
                defaultMQAdminExt.setMessageRequestMode(slave.getBrokerAddr(), topic, CONSUME_GROUP, MessageRequestMode.POP, 8, 60_000);
            }
        }

    }

}
