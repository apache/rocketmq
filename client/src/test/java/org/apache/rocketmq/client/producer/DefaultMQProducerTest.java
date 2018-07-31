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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerTest {
    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    private DefaultMQProducer producer;
    private Message message;
    private Message zeroMsg;
    private String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";

    @Before
    public void init() throws Exception {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        message = new Message(topic, new byte[] {'a'});
        zeroMsg = new Message(topic, new byte[] {});

        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        producer.getDefaultMQProducerImpl().getmQClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));
    }

    @After
    public void terminate() {
        producer.shutdown();
    }

    @Test
    public void testSendMessage_ZeroMessage() throws InterruptedException, RemotingException, MQBrokerException {
        try {
            producer.send(zeroMsg);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("message body length is zero");
        }
    }

    @Test
    public void testSendMessage_NoNameSrv() throws RemotingException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getNameServerAddressList()).thenReturn(new ArrayList<String>());
        try {
            producer.send(message);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("No name server address");
        }
    }

    @Test
    public void testSendMessage_NoRoute() throws RemotingException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getNameServerAddressList()).thenReturn(Collections.singletonList("127.0.0.1:9876"));
        try {
            producer.send(message);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("No route info of this topic");
        }
    }

    @Test
    public void testSendMessageSync_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(message);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }

    @Test
    public void testSendMessageSync_SuccessWithHook() throws Throwable {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final Throwable[] assertionErrors = new Throwable[1];
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "TestHook";
            }

            @Override
            public void sendMessageBefore(final SendMessageContext context) {
                assertionErrors[0] = assertInOtherThread(new Runnable() {
                    @Override
                    public void run() {
                        assertThat(context.getMessage()).isEqualTo(message);
                        assertThat(context.getProducer()).isEqualTo(producer);
                        assertThat(context.getCommunicationMode()).isEqualTo(CommunicationMode.SYNC);
                        assertThat(context.getSendResult()).isNull();
                    }
                });
                countDownLatch.countDown();
            }

            @Override
            public void sendMessageAfter(final SendMessageContext context) {
                assertionErrors[0] = assertInOtherThread(new Runnable() {
                    @Override
                    public void run() {
                        assertThat(context.getMessage()).isEqualTo(message);
                        assertThat(context.getProducer()).isEqualTo(producer.getDefaultMQProducerImpl());
                        assertThat(context.getCommunicationMode()).isEqualTo(CommunicationMode.SYNC);
                        assertThat(context.getSendResult()).isNotNull();
                    }
                });
                countDownLatch.countDown();
            }
        });
        SendResult sendResult = producer.send(message);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);

        countDownLatch.await();

        if (assertionErrors[0] != null) {
            throw assertionErrors[0];
        }
    }

    @Test
    public void testSetCallbackExecutor() throws MQClientException {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        ExecutorService customized = Executors.newCachedThreadPool();
        producer.setCallbackExecutor(customized);

        NettyRemotingClient remotingClient = (NettyRemotingClient) producer.getDefaultMQProducerImpl()
            .getmQClientFactory().getMQClientAPIImpl().getRemotingClient();

        assertThat(remotingClient.getCallbackExecutor()).isEqualTo(customized);
    }

    public static TopicRouteData createTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSynFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }

    private SendResult createSendResult(SendStatus sendStatus) {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("123");
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        return sendResult;
    }

    private Throwable assertInOtherThread(final Runnable runnable) {
        final Throwable[] assertionErrors = new Throwable[1];
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (AssertionError e) {
                    assertionErrors[0] = e;
                }
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            assertionErrors[0] = e;
        }
        return assertionErrors[0];
    }
}