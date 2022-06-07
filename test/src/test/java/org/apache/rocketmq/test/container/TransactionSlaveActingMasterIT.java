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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

//The test is correct, but it takes too much time, so it is ignored for the time being
@Ignore
public class TransactionSlaveActingMasterIT extends ContainerIntegrationTestBase {

    private static final String CONSUME_GROUP = TransactionSlaveActingMasterIT.class.getSimpleName() + "_Consumer";
    private final static int MESSAGE_COUNT = 32;
    private final static Random random = new Random();
    private static TransactionMQProducer producer;
    private static TransactionListenerImpl listener;
    private final static String MESSAGE_STRING = RandomStringUtils.random(1024);
    private static byte[] MESSAGE_BODY;

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
    }

    @BeforeClass
    public static void beforeClass() throws Throwable {
        listener = new TransactionListenerImpl();
        producer = new TransactionMQProducer(TransactionSlaveActingMasterIT.class.getSimpleName() + "_PRODUCER");
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(nsAddr);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        producer.setExecutorService(executorService);
        producer.setTransactionListener(listener);
        producer.setSendMsgTimeout(5000);
        producer.start();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        producer.shutdown();
    }

    @Test
    public void testLocalActing_delayMsg() throws Exception {
        awaitUntilSlaveOK();
        String topic = TransactionSlaveActingMasterIT.class.getSimpleName() + random.nextInt(65535);
        createTopic(topic);
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(topic, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        AtomicInteger inTimeMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            long period = System.currentTimeMillis() - msgs.get(0).getBornTimestamp();
            if (period - 120000 <= 4000) {
                inTimeMsgCount.addAndGet(msgs.size());
            }
            receivedMsgCount.addAndGet(msgs.size());
            msgs.forEach(x -> System.out.printf(x + "%n"));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });


        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic,"tag" + i, MESSAGE_BODY);
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
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

        System.out.printf("Remove master1%n");

        pushConsumer.start();

        await().atMost(Duration.ofMinutes(3)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT && inTimeMsgCount.get() >= MESSAGE_COUNT * 0.95);

        System.out.printf("consumer received %d msg, %d in time%n", receivedMsgCount.get(), inTimeMsgCount.get());

        pushConsumer.shutdown();

        //Add back master
        master1With3Replicas = brokerContainer1.addBroker(master1With3Replicas.getBrokerConfig(), master1With3Replicas.getMessageStoreConfig());
        master1With3Replicas.start();
        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("Add back master1%n");

        awaitUntilSlaveOK();
        // sleep a while to recover
        Thread.sleep(30000);
    }

    @Test
    public void testRemoteActing_delayMsg() throws Exception {
        awaitUntilSlaveOK();

        String topic = TransactionSlaveActingMasterIT.class.getSimpleName() + random.nextInt(65535);
        createTopic(topic);
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        AtomicInteger inTimeMsgCount = new AtomicInteger(0);
        AtomicInteger master3MsgCount = new AtomicInteger(0);

        int sendSuccess = 0;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic,"tag" + i, MESSAGE_BODY);
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                sendSuccess++;
            }
        }
        final int finalSendSuccess = sendSuccess;
        await().atMost(Duration.ofMinutes(1)).until(() -> finalSendSuccess >= MESSAGE_COUNT);
        long sendCompleteTimeStamp = System.currentTimeMillis();
        System.out.printf("send success%n");

        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(topic, "*");
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            long period = System.currentTimeMillis() - sendCompleteTimeStamp;
            // Remote Acting lead to born timestamp, msgId changed, it need to polish.
            if (period - 120000 <= 4000) {
                inTimeMsgCount.addAndGet(msgs.size());
            }
            if (msgs.get(0).getBrokerName().equals(master3With3Replicas.getBrokerConfig().getBrokerName())) {
                master3MsgCount.addAndGet(msgs.size());
            }
            receivedMsgCount.addAndGet(msgs.size());
            msgs.forEach(x -> System.out.printf("cost " + period + " " + x + "%n"));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        isolateBroker(master1With3Replicas);
        BrokerIdentity master1BrokerIdentity = new BrokerIdentity(
            master1With3Replicas.getBrokerConfig().getBrokerClusterName(),
            master1With3Replicas.getBrokerConfig().getBrokerName(),
            master1With3Replicas.getBrokerConfig().getBrokerId());

        brokerContainer1.removeBroker(master1BrokerIdentity);
        System.out.printf("Remove master1%n");

        isolateBroker(master2With3Replicas);
        BrokerIdentity master2BrokerIdentity = new BrokerIdentity(
            master2With3Replicas.getBrokerConfig().getBrokerClusterName(),
            master2With3Replicas.getBrokerConfig().getBrokerName(),
            master2With3Replicas.getBrokerConfig().getBrokerId());
        brokerContainer2.removeBroker(master2BrokerIdentity);
        System.out.printf("Remove master2%n");

        await().atMost(Duration.ofMinutes(3)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT && master3MsgCount.get() >= MESSAGE_COUNT && inTimeMsgCount.get() >= MESSAGE_COUNT * 0.95);

        System.out.printf("consumer received %d msg, %d in time%n", receivedMsgCount.get(), inTimeMsgCount.get());

        pushConsumer.shutdown();

        //Add back master
        master1With3Replicas = brokerContainer1.addBroker(master1With3Replicas.getBrokerConfig(), master1With3Replicas.getMessageStoreConfig());
        master1With3Replicas.start();
        cancelIsolatedBroker(master1With3Replicas);
        System.out.printf("Add back master1%n");

        //Add back master
        master2With3Replicas = brokerContainer2.addBroker(master2With3Replicas.getBrokerConfig(), master2With3Replicas.getMessageStoreConfig());
        master2With3Replicas.start();
        cancelIsolatedBroker(master2With3Replicas);
        System.out.printf("Add back master2%n");

        awaitUntilSlaveOK();
        // sleep a while to recover
        Thread.sleep(30000);
    }

    public static class TransactionListenerImpl implements TransactionListener {
        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            return LocalTransactionState.UNKNOW;
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
