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
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.consumer.MessageReceipt;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.message.Message;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.DefaultMessageReceipt;
import io.openmessaging.rocketmq.domain.DefaultQueueMetaData;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class PullConsumerImplTest {
    private PullConsumer pullConsumer;
    private String queueName = "HELLO_QUEUE";

    @Mock
    private DefaultMQPullConsumer rocketmqPullConsumer;
    private LocalMessageCache localMessageCache = null;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        final MessagingAccessPoint messagingAccessPoint = OMS
            .getMessagingAccessPoint("oms:rocketmq://IP1:9876,IP2:9876/namespace");
        final KeyValue attributes = messagingAccessPoint.attributes();
        attributes.put(NonStandardKeys.CONSUMER_ID, "TestGroup");
        pullConsumer = messagingAccessPoint.createPullConsumer();
        Set<String> queueNames = new HashSet<>(8);
        queueNames.add(queueName);
        pullConsumer.bindQueue(queueNames);

        Field field = PullConsumerImpl.class.getDeclaredField("rocketmqPullConsumer");
        field.setAccessible(true);
        field.set(pullConsumer, rocketmqPullConsumer); //Replace

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setOperationTimeout(200);
        localMessageCache = spy(new LocalMessageCache(rocketmqPullConsumer, clientConfig));

        field = PullConsumerImpl.class.getDeclaredField("localMessageCache");
        field.setAccessible(true);
        field.set(pullConsumer, localMessageCache);
        pullConsumer.start();
    }

    @Test
    public void testPoll() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        final byte[] testBody = new byte[] {'a', 'b'};
        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(testBody);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic(queueName);
        consumedMsg.setQueueId(0);
        consumedMsg.setStoreHost(new InetSocketAddress("127.0.0.1", 9876));
        doReturn(consumedMsg).when(localMessageCache).poll(any(KeyValue.class));
        Message message = pullConsumer.receive(3 * 1000);
        assertThat(message.header().getMessageId()).isEqualTo("NewMsgId");
        assertThat(message.getData()).isEqualTo(testBody);

        List<MessageExt> messageExts = new ArrayList<MessageExt>() {
            {
                add(consumedMsg);
            }
        };
        PullResult pullResult = new PullResult(PullStatus.FOUND, 11, 1, 100, messageExts);
        doReturn(pullResult).when(rocketmqPullConsumer).pull(any(MessageQueue.class), anyString(), anyLong(), anyInt(), anyLong());
        MessageQueue messageQueue = new MessageQueue(queueName, "breakeName", 0);
        Set<MessageQueue> messageQueues = new HashSet<MessageQueue>() {
            {
                add(messageQueue);
            }
        };
        doReturn(messageQueues).when(rocketmqPullConsumer).fetchSubscribeMessageQueues(queueName);
        QueueMetaData queueMetaData = new DefaultQueueMetaData(queueName, 0);
        MessageReceipt messageReceipt = new DefaultMessageReceipt("NewMsgId", 10L);
        long timeout = 3000L;
        Message message1 = pullConsumer.receive(queueName, queueMetaData, messageReceipt, timeout);
        assertThat(message1.header().getMessageId()).isEqualTo("NewMsgId");
        assertThat(message1.getData()).isEqualTo(testBody);
        assertThat(message1.header().getDestination()).isEqualTo(queueName);
        assertThat(message1.extensionHeader().getPartiton()).isEqualTo(0);
    }

    @Test
    public void testBatchPoll() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        final byte[] testBody = new byte[] {'a', 'b'};
        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(testBody);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic(queueName);
        consumedMsg.setQueueId(0);
        consumedMsg.setStoreHost(new InetSocketAddress("127.0.0.1", 9876));
        final byte[] testBody1 = new byte[] {'c', 'd'};
        MessageExt consumedMsg1 = new MessageExt();
        consumedMsg1.setMsgId("NewMsgId1");
        consumedMsg1.setBody(testBody1);
        consumedMsg1.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg1.setTopic(queueName);
        consumedMsg1.setQueueId(0);
        consumedMsg1.setStoreHost(new InetSocketAddress("127.0.0.1", 9876));
        List<MessageExt> messageExts = new ArrayList<MessageExt>() {
            {
                add(consumedMsg);
                add(consumedMsg1);
            }
        };
        doReturn(messageExts).when(localMessageCache).batchPoll(any(KeyValue.class));
        List<Message> messages = pullConsumer.batchReceive(3 * 1000);

        Message message1 = null;
        Message message2 = null;
        assertThat(messages.size()).isEqualTo(2);
        for (Message message : messages) {
            if (message.header().getMessageId().equals("NewMsgId")) {
                message1 = message;
            }
            if (message.header().getMessageId().equals("NewMsgId1")) {
                message2 = message;
            }
        }
        assertThat(message1).isNotNull();
        assertThat(message1.getData()).isEqualTo(testBody);
        assertThat(message1.header().getDestination()).isEqualTo(queueName);
        assertThat(message1.extensionHeader().getPartiton()).isEqualTo(0);
        assertThat(message2).isNotNull();
        assertThat(message2.getData()).isEqualTo(testBody1);
        assertThat(message2.header().getDestination()).isEqualTo(queueName);
        assertThat(message2.extensionHeader().getPartiton()).isEqualTo(0);

        PullResult pullResult = new PullResult(PullStatus.FOUND, 11, 1, 100, messageExts);
        doReturn(pullResult).when(rocketmqPullConsumer).pull(any(MessageQueue.class), anyString(), anyLong(), anyInt(), anyLong());
        MessageQueue messageQueue = new MessageQueue(queueName, "breakeName", 0);
        Set<MessageQueue> messageQueues = new HashSet<MessageQueue>() {
            {
                add(messageQueue);
            }
        };
        doReturn(messageQueues).when(rocketmqPullConsumer).fetchSubscribeMessageQueues(queueName);
        QueueMetaData queueMetaData = new DefaultQueueMetaData(queueName, 0);
        MessageReceipt messageReceipt = new DefaultMessageReceipt("NewMsgId", 10L);
        long timeout = 3000L;
        List<Message> message1s = pullConsumer.batchReceive(queueName, queueMetaData, messageReceipt, timeout);
        assertThat(message1s.size()).isEqualTo(2);
        Message message3 = null;
        Message message4 = null;
        for (Message message : message1s) {
            if (message.header().getMessageId().equals("NewMsgId")) {
                message3 = message;
            }
            if (message.header().getMessageId().equals("NewMsgId1")) {
                message4 = message;
            }
        }
        assertThat(message3).isNotNull();
        assertThat(message3.getData()).isEqualTo(testBody);
        assertThat(message3.header().getDestination()).isEqualTo(queueName);
        assertThat(message3.extensionHeader().getPartiton()).isEqualTo(0);
        assertThat(message4).isNotNull();
        assertThat(message4.getData()).isEqualTo(testBody1);
        assertThat(message4.header().getDestination()).isEqualTo(queueName);
        assertThat(message4.extensionHeader().getPartiton()).isEqualTo(0);
    }

    @Test
    public void testPoll_WithTimeout() {
        Message message = pullConsumer.receive(3 * 1000);
        assertThat(message).isNull();
    }
}