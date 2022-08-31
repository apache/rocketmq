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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Ignore
public class ScheduledMessageIT extends ContainerIntegrationTestBase {
    private static DefaultMQProducer producer;

    private static final String CONSUME_GROUP = ScheduledMessageIT.class.getSimpleName() + "_Consumer";
    private static final String MESSAGE_STRING = RandomStringUtils.random(1024);
    private static byte[] MESSAGE_BODY;

    static {
        try {
            MESSAGE_BODY = MESSAGE_STRING.getBytes(RemotingHelper.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException ignored) {
        }
    }

    private static final String TOPIC_PREFIX = ScheduledMessageIT.class.getSimpleName() + "_TOPIC";
    private static Random random = new Random();
    private static final int MESSAGE_COUNT = 128;

    public ScheduledMessageIT() throws UnsupportedEncodingException {
    }

    void createTopic(String topic) {
        createTopicTo(master1With3Replicas, topic, 1, 1);
        createTopicTo(master2With3Replicas, topic, 1, 1);
        createTopicTo(master3With3Replicas, topic, 1, 1);
    }

    @BeforeClass
    public static void beforeClass() throws Throwable {
        producer = createProducer(ScheduledMessageIT.class.getSimpleName() + "_PRODUCER");
        producer.setSendMsgTimeout(5000);
        producer.start();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        producer.shutdown();
    }

    @Ignore
    @Test
    public void consumeScheduledMsg() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String topic = TOPIC_PREFIX + random.nextInt(65535);
        createTopic(topic);
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP + random.nextInt(65535));
        pushConsumer.subscribe(topic, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        AtomicInteger inTimeMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            long period = System.currentTimeMillis() - msgs.get(0).getBornTimestamp();
            if (Math.abs(period - 5000) <= 1000) {
                inTimeMsgCount.addAndGet(msgs.size());
            }
            receivedMsgCount.addAndGet(msgs.size());
            msgs.forEach(x -> System.out.printf(receivedMsgCount.get() + " cost " + period + " " + x + "%n"));

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, MESSAGE_BODY);
            msg.setDelayTimeLevel(2);
            producer.send(msg);
        }

        await().atMost(Duration.ofSeconds(MESSAGE_COUNT * 2)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT && inTimeMsgCount.get() >= MESSAGE_COUNT * 0.95);

        System.out.printf("consumer received %d msg, %d in time%n", receivedMsgCount.get(), inTimeMsgCount.get());

        pushConsumer.shutdown();
    }

    @Test
    public void consumeScheduledMsgFromSlave() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String topic = TOPIC_PREFIX + random.nextInt(65535);
        createTopic(topic);
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP + random.nextInt(65535));
        pushConsumer.subscribe(topic, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            receivedMsgCount.addAndGet(msgs.size());
            msgs.forEach(x -> System.out.printf(x + "%n"));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, String.valueOf(i).getBytes());
            msg.setDelayTimeLevel(2);
            producer.send(msg);
        }

        isolateBroker(master1With3Replicas);

        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
        assertThat(producer.getDefaultMQProducerImpl().getmQClientFactory().findBrokerAddressInPublish(topic)).isNull();

        pushConsumer.start();

        await().atMost(Duration.ofSeconds(MESSAGE_COUNT * 2)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT);

        pushConsumer.shutdown();
        cancelIsolatedBroker(master1With3Replicas);

        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 2);
    }

    @Test
    public void consumeTimerMsgFromSlave() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String topic = TOPIC_PREFIX + random.nextInt(65535);
        createTopic(topic);
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(topic, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            receivedMsgCount.addAndGet(msgs.size());
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(topic, String.valueOf(i).getBytes());
            msg.setDelayTimeSec(3);
            producer.send(msg);
        }

        isolateBroker(master1With3Replicas);

        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
        assertThat(producer.getDefaultMQProducerImpl().getmQClientFactory().findBrokerAddressInPublish(topic)).isNull();

        pushConsumer.start();

        await().atMost(Duration.ofSeconds(MESSAGE_COUNT * 2)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT);

        pushConsumer.shutdown();
        cancelIsolatedBroker(master1With3Replicas);

        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 2);
    }

}
