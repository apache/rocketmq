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
package org.apache.rocketmq.client.producer;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQRedirectException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.assertj.core.api.ThrowableAssert;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerLogicalQueueTest {
    private MQClientInstance mQClientFactory;
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    private DefaultMQProducer producer;
    private Message message;
    private String topic;

    private MessageQueue messageQueue;

    private static final String cluster = "DefaultCluster";
    private static final String broker1Name = "broker1";
    private static final String broker2Name = "broker2";
    private static final String broker1Addr = "127.0.0.2:10911";
    private static final String broker2Addr = "127.0.0.3:10911";

    @Before
    public void init() throws Exception {
        topic = "Foobar" + System.nanoTime();
        messageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, 0);

        ConcurrentMap<String, MQClientInstance> factoryTable = (ConcurrentMap<String/* clientId */, MQClientInstance>) FieldUtils.readDeclaredField(MQClientManager.getInstance(), "factoryTable", true);
        for (MQClientInstance instance : factoryTable.values()) {
            instance.shutdown();
        }
        factoryTable.clear();

        mQClientFactory = spy(MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig()));
        factoryTable.put(new ClientConfig().buildMQClientId(), mQClientFactory);

        String producerGroupTemp = "FooBar_PID" + System.nanoTime();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);
        message = new Message(topic, new byte[] {'a'});

        mQClientFactory.registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        producer.start();

        FieldUtils.writeDeclaredField(producer.getDefaultMQProducerImpl(), "mQClientFactory", mQClientFactory, true);
        FieldUtils.writeField(mQClientFactory, "mQClientAPIImpl", mQClientAPIImpl, true);

        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            (SendCallback) isNull(), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));
        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            any(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenAnswer(new Answer<SendResult>() {
                @Override public SendResult answer(InvocationOnMock invocation) throws Throwable {
                    SendCallback sendCallback = invocation.getArgument(6);
                    sendCallback.onSuccess(DefaultMQProducerLogicalQueueTest.this.createSendResult(SendStatus.SEND_OK));
                    return null;
                }
            });
    }

    @After
    public void terminate() {
        producer.shutdown();
    }

    @Test
    public void testSendMessageSync_Success() throws Exception {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong(), anyBoolean(), ArgumentMatchers.<Set<Integer>>any())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(message, messageQueue);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }

    @Test
    public void testSendMessageSync_Redirect() throws Exception {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong(), anyBoolean(), ArgumentMatchers.<Set<Integer>>any())).thenReturn(createTopicRoute());

        when(mQClientAPIImpl.sendMessage(eq(broker1Addr), eq(broker1Name), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC),
            (SendCallback) isNull(), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenThrow(new MQRedirectException(null));

        assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
            @Override public void call() throws Throwable {
                producer.send(message, messageQueue);
            }
        }).isInstanceOf(MQBrokerException.class).hasMessageContaining("redirect");

        when(mQClientAPIImpl.sendMessage(eq(broker1Addr), eq(broker1Name), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC),
            (SendCallback) isNull(), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenThrow(new MQRedirectException(JSON.toJSONBytes(ImmutableList.of(
                new LogicalQueueRouteData(0, 0, new MessageQueue(topic, broker1Name, 0), MessageQueueRouteState.Expired, 0, 0, 0, 0, broker1Addr),
                new LogicalQueueRouteData(0, 10, new MessageQueue(topic, broker2Name, 0), MessageQueueRouteState.Normal, 0, -1, -1, -1, broker2Addr)))));
        when(mQClientAPIImpl.sendMessage(eq(broker2Addr), eq(broker2Name), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC),
            (SendCallback) isNull(), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));

        SendResult sendResult = producer.send(message, messageQueue);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(466L);
    }

    @Test
    public void testSendMessageSync_RemotingException() throws Exception {
        TopicRouteData topicRouteData = createTopicRoute();
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong(), anyBoolean(), ArgumentMatchers.<Set<Integer>>any())).thenReturn(topicRouteData);

        when(mQClientAPIImpl.sendMessage(eq(broker1Addr), eq(broker1Name), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC),
            (SendCallback) isNull(), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenThrow(new RemotingConnectException(broker1Addr));
        SendResult returnSendResult = createSendResult(SendStatus.SEND_OK);
        when(mQClientAPIImpl.sendMessage(eq(broker2Addr), eq(broker2Name), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), eq(CommunicationMode.SYNC),
            (SendCallback) isNull(), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(returnSendResult);

        assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
            @Override public void call() throws Throwable {
                producer.send(message, messageQueue);
            }
        }).isInstanceOf(RemotingConnectException.class).hasMessageContaining(broker1Addr);

        topicRouteData.getLogicalQueuesInfo().get(0).add(new LogicalQueueRouteData(0, -1, new MessageQueue(topic, broker2Name, 1), MessageQueueRouteState.WriteOnly, 0, -1, -1, -1, broker2Addr));

        SendResult sendResult = producer.send(message, messageQueue);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(-1L);
    }

    @Test
    public void testSendMessageAsync_Success() throws Exception {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong(), anyBoolean(), ArgumentMatchers.<Set<Integer>>any())).thenReturn(createTopicRoute());

        final SettableFuture<SendResult> future = SettableFuture.create();
        producer.send(message, messageQueue, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                future.set(sendResult);
            }

            @Override
            public void onException(Throwable e) {
                future.setException(e);
            }
        });

        SendResult sendResult = future.get(3, TimeUnit.SECONDS);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }

    @Test
    public void testSendMessageAsync() throws Exception {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong(), anyBoolean(), ArgumentMatchers.<Set<Integer>>any())).thenReturn(createTopicRoute());

        final AtomicReference<SettableFuture<SendResult>> future = new AtomicReference<SettableFuture<SendResult>>();
        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                future.get().set(sendResult);
            }

            @Override
            public void onException(Throwable e) {
                future.get().setException(e);
            }
        };

        Message message = new Message();
        message.setTopic("test");
        message.setBody("hello world".getBytes());
        future.set(SettableFuture.<SendResult>create());
        producer.send(new Message(), messageQueue, sendCallback);
        assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
            @Override public void call() throws Throwable {
                future.get().get(3, TimeUnit.SECONDS);
            }
        }).hasCauseInstanceOf(MQClientException.class).hasMessageContaining("The specified topic is blank");

        //this message is send success
        message.setTopic(topic);
        future.set(SettableFuture.<SendResult>create());
        producer.send(message, messageQueue, sendCallback, 1000);
        future.get().get(3, TimeUnit.SECONDS);
    }

    public TopicRouteData createTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setBrokerDatas(ImmutableList.of(
            new BrokerData(cluster, broker1Name, new HashMap<Long, String>(Collections.singletonMap(MixAll.MASTER_ID, broker1Addr))),
            new BrokerData(cluster, broker2Name, new HashMap<Long, String>(Collections.singletonMap(MixAll.MASTER_ID, broker2Addr)))
        ));

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData;
        queueData = new QueueData();
        queueData.setBrokerName(broker1Name);
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        queueData = new QueueData();
        queueData.setBrokerName(broker2Name);
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);

        LogicalQueuesInfo info = new LogicalQueuesInfo();
        info.put(0, Lists.newArrayList(new LogicalQueueRouteData(0, 0, new MessageQueue(topic, broker1Name, 0), MessageQueueRouteState.Normal, 0, 0, 0, 0, broker1Addr)));
        topicRouteData.setLogicalQueuesInfo(info);
        return topicRouteData;
    }

    private SendResult createSendResult(SendStatus sendStatus) {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("123");
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        sendResult.setMessageQueue(new MessageQueue(topic, broker1Name, 0));
        return sendResult;
    }
}
