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
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.container.InnerSalveBrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
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
public class PullMultipleReplicasIT extends ContainerIntegrationTestBase {
    private static DefaultMQPullConsumer pullConsumer;
    private static DefaultMQProducer producer;
    private static MQClientInstance mqClientInstance;

    private static final String MESSAGE_STRING = RandomStringUtils.random(1024);
    private static final byte[] MESSAGE_BODY = MESSAGE_STRING.getBytes(StandardCharsets.UTF_8);

    public PullMultipleReplicasIT() {
    }

    @BeforeClass
    public static void beforeClass() throws Exception {

        pullConsumer = createPullConsumer(PullMultipleReplicasIT.class.getSimpleName() + "_Consumer");
        pullConsumer.start();

        Field field = DefaultMQPullConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        mqClientInstance = (MQClientInstance) field.get(pullConsumer.getDefaultMQPullConsumerImpl());

        producer = createProducer(PullMultipleReplicasIT.class.getSimpleName() + "_Producer");
        producer.setSendMsgTimeout(15 * 1000);
        producer.start();
    }

    @AfterClass
    public static void afterClass() {
        producer.shutdown();
        pullConsumer.shutdown();
    }

    @Test
    public void testPullMessageFromSlave() throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
        awaitUntilSlaveOK();

        Message msg = new Message(THREE_REPLICAS_TOPIC, MESSAGE_BODY);
        SendResult sendResult = producer.send(msg);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);

        final MessageQueue messageQueue = sendResult.getMessageQueue();
        final long queueOffset = sendResult.getQueueOffset();

        final PullResult[] pullResult = {null};
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            pullResult[0] = pullConsumer.pull(messageQueue, "*", queueOffset, 1);
            return pullResult[0].getPullStatus() == PullStatus.FOUND;
        });

        List<MessageExt> msgFoundList = pullResult[0].getMsgFoundList();
        assertThat(msgFoundList.size()).isEqualTo(1);
        assertThat(new String(msgFoundList.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET)).isEqualTo(MESSAGE_STRING);

        // Pull the same message from the slave broker
        pullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper().updatePullFromWhichNode(messageQueue, 1);

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            pullResult[0] = pullConsumer.pull(messageQueue, "*", queueOffset, 1);
            return pullResult[0].getPullStatus() == PullStatus.FOUND;
        });

        msgFoundList = pullResult[0].getMsgFoundList();
        assertThat(msgFoundList.size()).isEqualTo(1);
        assertThat(new String(msgFoundList.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET)).isEqualTo(MESSAGE_STRING);

        // Pull the same message from the slave broker
        pullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper().updatePullFromWhichNode(messageQueue, 2);

        await().atMost(Duration.ofSeconds(5)).until(() -> {
            pullResult[0] = pullConsumer.pull(messageQueue, "*", queueOffset, 1);
            return pullResult[0].getPullStatus() == PullStatus.FOUND;
        });

        msgFoundList = pullResult[0].getMsgFoundList();
        assertThat(msgFoundList.size()).isEqualTo(1);
        assertThat(new String(msgFoundList.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET)).isEqualTo(MESSAGE_STRING);

        pullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper().updatePullFromWhichNode(messageQueue, 0);
    }

    @Test
    public void testSendMessageBackToSlave() throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
        awaitUntilSlaveOK();

        String clusterTopic = "TOPIC_ON_BROKER2_AND_BROKER3_FOR_MESSAGE_BACK";
        createTopicTo(master1With3Replicas, clusterTopic);
        createTopicTo(master3With3Replicas, clusterTopic);

        Message msg = new Message(clusterTopic, MESSAGE_BODY);
        producer.setSendMsgTimeout(10 * 1000);

        final MessageQueue[] selectedQueue = new MessageQueue[1];
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            for (final MessageQueue queue : producer.fetchPublishMessageQueues(clusterTopic)) {
                if (queue.getBrokerName().equals(master3With3Replicas.getBrokerConfig().getBrokerName())) {
                    selectedQueue[0] = queue;
                }
            }
            return selectedQueue[0] != null;
        });

        SendResult sendResult = producer.send(msg, selectedQueue[0]);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);

        final MessageQueue messageQueue = sendResult.getMessageQueue();
        final long queueOffset = sendResult.getQueueOffset();

        final PullResult[] pullResult = {null};
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            pullResult[0] = pullConsumer.pull(messageQueue, "*", queueOffset, 1);
            return pullResult[0].getPullStatus() == PullStatus.FOUND;
        });

        await().atMost(Duration.ofSeconds(60)).until(() -> {
            DefaultMessageStore messageStore = (DefaultMessageStore) master3With3Replicas.getMessageStore();
            return messageStore.getHaService().inSyncReplicasNums(messageStore.getMaxPhyOffset()) == 3;
        });

        InnerSalveBrokerController slaveBroker = null;
        for (InnerSalveBrokerController slave : brokerContainer1.getSlaveBrokers()) {
            if (slave.getBrokerConfig().getBrokerName().equals(master3With3Replicas.getBrokerConfig().getBrokerName())) {
                slaveBroker = slave;
            }
        }

        assertThat(slaveBroker).isNotNull();

        MessageExt backMessage = pullResult[0].getMsgFoundList().get(0);

        // Message will be sent to the master broker(master1With3Replicas) beside a slave broker of master3With3Replicas
        backMessage.setStoreHost(new InetSocketAddress(slaveBroker.getBrokerConfig().getBrokerIP1(), slaveBroker.getBrokerConfig().getListenPort()));
        pullConsumer.sendMessageBack(backMessage, 0);

        String retryTopic = MixAll.getRetryTopic(pullConsumer.getConsumerGroup());
        // Retry topic only has one queue by default
        MessageQueue newMsgQueue = new MessageQueue(retryTopic, master1With3Replicas.getBrokerConfig().getBrokerName(), 0);
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            pullResult[0] = pullConsumer.pull(newMsgQueue, "*", 0, 1);
            return pullResult[0].getPullStatus() == PullStatus.FOUND;
        });

        List<MessageExt> msgFoundList = pullResult[0].getMsgFoundList();
        assertThat(msgFoundList.size()).isEqualTo(1);
        assertThat(new String(msgFoundList.get(0).getBody(), RemotingHelper.DEFAULT_CHARSET)).isEqualTo(MESSAGE_STRING);

        awaitUntilSlaveOK();
    }
}
