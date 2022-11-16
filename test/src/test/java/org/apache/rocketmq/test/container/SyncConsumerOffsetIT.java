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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.container.BrokerContainer;
import org.apache.rocketmq.container.InnerSalveBrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Ignore
public class SyncConsumerOffsetIT extends ContainerIntegrationTestBase {
    private static final String THREE_REPLICA_CONSUMER_GROUP = "SyncConsumerOffsetIT_ConsumerThreeReplica";
    private static final String TEST_SYNC_TOPIC = SyncConsumerOffsetIT.class.getSimpleName() + "_topic";

    private static DefaultMQProducer mqProducer;
    private static DefaultMQPushConsumer mqConsumerThreeReplica;
    private static final String MSG = "Hello RocketMQ ";
    private static final byte[] MESSAGE_BODY = MSG.getBytes(StandardCharsets.UTF_8);

    public SyncConsumerOffsetIT() {
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        createTopicTo(master3With3Replicas, TEST_SYNC_TOPIC);

        mqProducer = createProducer("SyncConsumerOffsetIT_Producer");
        mqProducer.setSendMsgTimeout(15 * 1000);
        mqProducer.start();

        mqConsumerThreeReplica = createPushConsumer(THREE_REPLICA_CONSUMER_GROUP);
        mqConsumerThreeReplica.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqConsumerThreeReplica.subscribe(TEST_SYNC_TOPIC, "*");
    }

    @AfterClass
    public static void afterClass() {
        mqProducer.shutdown();
        mqConsumerThreeReplica.shutdown();
    }

    @Test
    public void syncConsumerOffsetWith3Replicas() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        syncConsumeOffsetInner(TEST_SYNC_TOPIC, mqConsumerThreeReplica,
            master3With3Replicas, Arrays.asList(brokerContainer1, brokerContainer2));
    }

    private void syncConsumeOffsetInner(String topic, DefaultMQPushConsumer consumer, BrokerController master,
        List<BrokerContainer> slaveContainers) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        awaitUntilSlaveOK();
        String group = THREE_REPLICA_CONSUMER_GROUP;

        int msgCount = 100;
        for (int i = 0; i < msgCount; i++) {
            Message msg = new Message(topic, MESSAGE_BODY);
            SendResult sendResult = mqProducer.send(msg);
            assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        }

        CountDownLatch countDownLatch = new CountDownLatch(msgCount);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            countDownLatch.countDown();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        boolean ok = countDownLatch.await(100, TimeUnit.SECONDS);
        assertThat(ok).isEqualTo(true);
        System.out.printf("consume complete%n");

        final Set<MessageQueue> mqSet = filterMessageQueue(consumer.fetchSubscribeMessageQueues(topic), topic);

        await().atMost(120, TimeUnit.SECONDS).until(() -> {
            Map<Integer, Long> consumerOffsetMap = new HashMap<>();
            long offsetTotal = 0L;
            for (MessageQueue mq : mqSet) {
                long queueOffset = master.getConsumerOffsetManager().queryOffset(group, topic, mq.getQueueId());
                if (queueOffset < 0) {
                    continue;
                }
                offsetTotal += queueOffset;
                consumerOffsetMap.put(mq.getQueueId(), queueOffset);
            }

            if (offsetTotal < 100) {
                return false;
            }
            boolean syncOk = true;

            for (BrokerContainer brokerContainer : slaveContainers) {
                for (InnerSalveBrokerController slave : brokerContainer.getSlaveBrokers()) {
                    if (!slave.getBrokerConfig().getBrokerName().equals(master.getBrokerConfig().getBrokerName())) {
                        continue;
                    }
                    for (MessageQueue mq : mqSet) {
                        long slaveOffset = slave.getConsumerOffsetManager().queryOffset(group, topic, mq.getQueueId());
                        boolean check = slaveOffset == consumerOffsetMap.get(mq.getQueueId());
                        syncOk &= check;
                    }
                }
            }

            return syncOk;
        });
    }
}
