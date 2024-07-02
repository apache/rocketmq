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

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.compression.CompressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerTest {
    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    private DefaultMQProducer producer;
    private Message message;
    private Message zeroMsg;
    private Message bigMessage;
    private final String topic = "FooBar";
    private final String producerGroupPrefix = "FooBar_PID";
    private final long defaultTimeout = 3000L;

    @Before
    public void init() throws Exception {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setCompressMsgBodyOverHowmuch(16);
        message = new Message(topic, new byte[] {'a'});
        zeroMsg = new Message(topic, new byte[] {});
        bigMessage = new Message(topic, "This is a very huge message!".getBytes());

        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        producer.getDefaultMQProducerImpl().getMqClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

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
        when(mQClientAPIImpl.getNameServerAddressList()).thenReturn(new ArrayList<>());
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
    public void testSendMessageSync_WithBodyCompressed() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(bigMessage);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }

    @Test
    public void testSendMessageAsync_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSendMessageAsync() throws RemotingException, MQClientException, InterruptedException {
        final AtomicInteger cc = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(12);

        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                cc.incrementAndGet();
                countDownLatch.countDown();
            }
        };
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return null;
            }
        };

        // on enableBackpressureForAsyncMode
        producer.setEnableBackpressureForAsyncMode(true);
        producer.setBackPressureForAsyncSendNum(5000);
        producer.setBackPressureForAsyncSendSize(50 * 1024 * 1024);
        Message message = new Message();
        message.setTopic("test");
        message.setBody("hello world".getBytes());
        producer.send(new Message(), sendCallback);
        producer.send(message, new MessageQueue(), sendCallback);
        producer.send(new Message(), new MessageQueue(), sendCallback, 1000);
        producer.send(new Message(), messageQueueSelector, null, sendCallback);
        producer.send(message, messageQueueSelector, null, sendCallback, 1000);
        //this message is send success
        producer.send(message, sendCallback, 1000);

        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(5);

        // off enableBackpressureForAsyncMode
        producer.setEnableBackpressureForAsyncMode(false);
        producer.send(new Message(), sendCallback);
        producer.send(message, new MessageQueue(), sendCallback);
        producer.send(new Message(), new MessageQueue(), sendCallback, 1000);
        producer.send(new Message(), messageQueueSelector, null, sendCallback);
        producer.send(message, messageQueueSelector, null, sendCallback, 1000);
        //this message is send success
        producer.send(message, sendCallback, 1000);

        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(10);
    }

    @Test
    public void testBatchSendMessageAsync()
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        final AtomicInteger cc = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(4);

        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                cc.incrementAndGet();
                countDownLatch.countDown();
            }
        };
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return null;
            }
        };

        List<Message> msgs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Message message = new Message();
            message.setTopic("test");
            message.setBody(("hello world" + i).getBytes());
            msgs.add(message);
        }

        // on enableBackpressureForAsyncMode
        producer.setEnableBackpressureForAsyncMode(true);
        producer.send(msgs, sendCallback);
        producer.send(msgs, sendCallback, 1000);
        MessageQueue mq = new MessageQueue("test", "BrokerA", 1);
        producer.send(msgs, mq, sendCallback);
        // this message is send failed
        producer.send(msgs, new MessageQueue(), sendCallback, 1000);

        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(1);

        // off enableBackpressureForAsyncMode
        producer.setEnableBackpressureForAsyncMode(false);
        producer.send(msgs, sendCallback);
        producer.send(msgs, sendCallback, 1000);
        producer.send(msgs, mq, sendCallback);
        // this message is send failed
        producer.send(msgs, new MessageQueue(), sendCallback, 1000);

        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(2);
    }

    @Test
    public void testSendMessageAsync_BodyCompressed() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        producer.send(bigMessage, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
            }
        });
        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
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
        String producerGroupTemp = "testSetCallbackExecutor_" + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        ExecutorService customized = Executors.newCachedThreadPool();
        producer.setCallbackExecutor(customized);

        NettyRemotingClient remotingClient = (NettyRemotingClient) producer.getDefaultMQProducerImpl()
            .getMqClientFactory().getMQClientAPIImpl().getRemotingClient();

        assertThat(remotingClient.getCallbackExecutor()).isEqualTo(customized);
    }

    @Test
    public void testRequestMessage() throws RemotingException, RequestTimeoutException, MQClientException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final AtomicBoolean finish = new AtomicBoolean(false);
        new Thread(new Runnable() {
            @Override
            public void run() {
                ConcurrentHashMap<String, RequestResponseFuture> responseMap = RequestFutureHolder.getInstance().getRequestFutureTable();
                assertThat(responseMap).isNotNull();
                while (!finish.get()) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                    MessageExt responseMsg = new MessageExt();
                    responseMsg.setTopic(message.getTopic());
                    responseMsg.setBody(message.getBody());
                    for (Map.Entry<String, RequestResponseFuture> entry : responseMap.entrySet()) {
                        RequestResponseFuture future = entry.getValue();
                        future.putResponseMessage(responseMsg);
                    }
                }
            }
        }).start();
        Message result = producer.request(message, 3 * 1000L);
        finish.getAndSet(true);
        assertThat(result).isExactlyInstanceOf(MessageExt.class);
        assertThat(result.getTopic()).isEqualTo("FooBar");
        assertThat(result.getBody()).isEqualTo(new byte[] {'a'});
    }

    @Test(expected = RequestTimeoutException.class)
    public void testRequestMessage_RequestTimeoutException() throws RemotingException, RequestTimeoutException, MQClientException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        Message result = producer.request(message, 3 * 1000L);
    }

    @Test
    public void testAsyncRequest_OnSuccess() throws Exception {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        RequestCallback requestCallback = new RequestCallback() {
            @Override
            public void onSuccess(Message message) {
                assertThat(message).isExactlyInstanceOf(MessageExt.class);
                assertThat(message.getTopic()).isEqualTo("FooBar");
                assertThat(message.getBody()).isEqualTo(new byte[] {'a'});
                assertThat(message.getFlag()).isEqualTo(1);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
            }
        };
        producer.request(message, requestCallback, 3 * 1000L);
        ConcurrentHashMap<String, RequestResponseFuture> responseMap = RequestFutureHolder.getInstance().getRequestFutureTable();
        assertThat(responseMap).isNotNull();

        MessageExt responseMsg = new MessageExt();
        responseMsg.setTopic(message.getTopic());
        responseMsg.setBody(message.getBody());
        responseMsg.setFlag(1);
        for (Map.Entry<String, RequestResponseFuture> entry : responseMap.entrySet()) {
            RequestResponseFuture future = entry.getValue();
            future.setSendRequestOk(true);
            future.getRequestCallback().onSuccess(responseMsg);
        }
        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testAsyncRequest_OnException() throws Exception {
        final AtomicInteger cc = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        RequestCallback requestCallback = new RequestCallback() {
            @Override
            public void onSuccess(Message message) {

            }

            @Override
            public void onException(Throwable e) {
                cc.incrementAndGet();
                countDownLatch.countDown();
            }
        };
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return null;
            }
        };

        try {
            producer.request(message, requestCallback, 3 * 1000L);
            failBecauseExceptionWasNotThrown(Exception.class);
        } catch (Exception e) {
            ConcurrentHashMap<String, RequestResponseFuture> responseMap = RequestFutureHolder.getInstance().getRequestFutureTable();
            assertThat(responseMap).isNotNull();
            for (Map.Entry<String, RequestResponseFuture> entry : responseMap.entrySet()) {
                RequestResponseFuture future = entry.getValue();
                future.getRequestCallback().onException(e);
            }
        }
        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(1);
    }

    @Test
    public void testBatchSendMessageAsync_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        producer.setAutoBatch(true);
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                countDownLatch.countDown();
            }
        });

        countDownLatch.await(defaultTimeout, TimeUnit.MILLISECONDS);
        producer.setAutoBatch(false);
    }

    @Test
    public void testBatchSendMessageSync_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        producer.setAutoBatch(true);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(message);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
        producer.setAutoBatch(false);
    }


    @Test
    public void testRunningSetBackCompress() throws RemotingException, InterruptedException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(5);
        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                countDownLatch.countDown();
            }
        };

        // on enableBackpressureForAsyncMode
        producer.setEnableBackpressureForAsyncMode(true);
        producer.setBackPressureForAsyncSendNum(10);
        producer.setBackPressureForAsyncSendSize(50 * 1024 * 1024);
        Message message = new Message();
        message.setTopic("test");
        message.setBody("hello world".getBytes());
        MessageQueue mq = new MessageQueue("test", "BrokerA", 1);
        //this message is send success
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        producer.send(message, mq, sendCallback);
                    } catch (MQClientException | RemotingException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();
        }
        producer.setBackPressureForAsyncSendNum(15);
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
        assertThat(producer.defaultMQProducerImpl.getSemaphoreAsyncSendNumAvailablePermits() + countDownLatch.getCount()).isEqualTo(15);
        producer.setEnableBackpressureForAsyncMode(false);
    }

    public static TopicRouteData createTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<>());
        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
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

    @Test
    public void assertCreateDefaultMQProducer() {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        DefaultMQProducer producer1 = new DefaultMQProducer(producerGroupTemp);
        assertNotNull(producer1);
        assertEquals(producerGroupTemp, producer1.getProducerGroup());
        assertNotNull(producer1.getDefaultMQProducerImpl());
        assertTrue(producer1.getTotalBatchMaxBytes() > 0);
        assertTrue(producer1.getBatchMaxBytes() > 0);
        assertTrue(producer1.getBatchMaxDelayMs() > 0);
        assertNull(producer1.getTopics());
        assertFalse(producer1.isEnableTrace());
        assertTrue(UtilAll.isBlank(producer1.getTraceTopic()));
        DefaultMQProducer producer2 = new DefaultMQProducer(producerGroupTemp, mock(RPCHook.class));
        assertNotNull(producer2);
        assertEquals(producerGroupTemp, producer2.getProducerGroup());
        assertNotNull(producer2.getDefaultMQProducerImpl());
        assertTrue(producer2.getTotalBatchMaxBytes() > 0);
        assertTrue(producer2.getBatchMaxBytes() > 0);
        assertTrue(producer2.getBatchMaxDelayMs() > 0);
        assertNull(producer2.getTopics());
        assertFalse(producer2.isEnableTrace());
        assertTrue(UtilAll.isBlank(producer2.getTraceTopic()));
        DefaultMQProducer producer3 = new DefaultMQProducer(producerGroupTemp, mock(RPCHook.class), Collections.singletonList("custom_topic"));
        assertNotNull(producer3);
        assertEquals(producerGroupTemp, producer3.getProducerGroup());
        assertNotNull(producer3.getDefaultMQProducerImpl());
        assertTrue(producer3.getTotalBatchMaxBytes() > 0);
        assertTrue(producer3.getBatchMaxBytes() > 0);
        assertTrue(producer3.getBatchMaxDelayMs() > 0);
        assertNotNull(producer3.getTopics());
        assertEquals(1, producer3.getTopics().size());
        assertFalse(producer3.isEnableTrace());
        assertTrue(UtilAll.isBlank(producer3.getTraceTopic()));
        DefaultMQProducer producer4 = new DefaultMQProducer(producerGroupTemp, mock(RPCHook.class), true, "custom_trace_topic");
        assertNotNull(producer4);
        assertEquals(producerGroupTemp, producer4.getProducerGroup());
        assertNotNull(producer4.getDefaultMQProducerImpl());
        assertTrue(producer4.getTotalBatchMaxBytes() > 0);
        assertTrue(producer4.getBatchMaxBytes() > 0);
        assertTrue(producer4.getBatchMaxDelayMs() > 0);
        assertNull(producer4.getTopics());
        assertTrue(producer4.isEnableTrace());
        assertEquals("custom_trace_topic", producer4.getTraceTopic());
        DefaultMQProducer producer5 = new DefaultMQProducer(producerGroupTemp, mock(RPCHook.class), Collections.singletonList("custom_topic"), true, "custom_trace_topic");
        assertNotNull(producer5);
        assertEquals(producerGroupTemp, producer5.getProducerGroup());
        assertNotNull(producer5.getDefaultMQProducerImpl());
        assertTrue(producer5.getTotalBatchMaxBytes() > 0);
        assertTrue(producer5.getBatchMaxBytes() > 0);
        assertTrue(producer5.getBatchMaxDelayMs() > 0);
        assertNotNull(producer5.getTopics());
        assertEquals(1, producer5.getTopics().size());
        assertTrue(producer5.isEnableTrace());
        assertEquals("custom_trace_topic", producer5.getTraceTopic());
    }

    @Test
    public void assertSend() throws MQBrokerException, RemotingException, InterruptedException, MQClientException, NoSuchFieldException, IllegalAccessException {
        setDefaultMQProducerImpl();
        setOtherParam();
        SendResult send = producer.send(message, defaultTimeout);
        assertNull(send);
        Collection<Message> msgs = Collections.singletonList(message);
        send = producer.send(msgs);
        assertNull(send);
        send = producer.send(msgs, defaultTimeout);
        assertNull(send);
    }

    @Test
    public void assertSendOneway() throws RemotingException, InterruptedException, MQClientException, NoSuchFieldException, IllegalAccessException {
        setDefaultMQProducerImpl();
        producer.sendOneway(message);
        MessageQueue mq = mock(MessageQueue.class);
        producer.sendOneway(message, mq);
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        producer.sendOneway(message, selector, 1);
    }

    @Test
    public void assertSendByQueue() throws MQBrokerException, RemotingException, InterruptedException, MQClientException, NoSuchFieldException, IllegalAccessException {
        setDefaultMQProducerImpl();
        MessageQueue mq = mock(MessageQueue.class);
        SendResult send = producer.send(message, mq);
        assertNull(send);
        send = producer.send(message, mq, defaultTimeout);
        assertNull(send);
        Collection<Message> msgs = Collections.singletonList(message);
        send = producer.send(msgs, mq);
        assertNull(send);
        send = producer.send(msgs, mq, defaultTimeout);
        assertNull(send);
    }

    @Test
    public void assertSendByQueueSelector() throws MQBrokerException, RemotingException, InterruptedException, MQClientException, NoSuchFieldException, IllegalAccessException {
        setDefaultMQProducerImpl();
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        SendResult send = producer.send(message, selector, 1);
        assertNull(send);
        send = producer.send(message, selector, 1, defaultTimeout);
        assertNull(send);
    }

    @Test
    public void assertRequest() throws MQBrokerException, RemotingException, InterruptedException, MQClientException, NoSuchFieldException, IllegalAccessException, RequestTimeoutException {
        setDefaultMQProducerImpl();
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        Message replyNsg = producer.request(message, selector, 1, defaultTimeout);
        assertNull(replyNsg);
        RequestCallback requestCallback = mock(RequestCallback.class);
        producer.request(message, selector, 1, requestCallback, defaultTimeout);
        MessageQueue mq = mock(MessageQueue.class);
        producer.request(message, mq, defaultTimeout);
        producer.request(message, mq, requestCallback, defaultTimeout);
    }

    @Test(expected = RuntimeException.class)
    public void assertSendMessageInTransaction() throws MQClientException {
        TransactionSendResult result = producer.sendMessageInTransaction(message, 1);
        assertNull(result);
    }

    @Test
    public void assertSearchOffset() throws MQClientException, NoSuchFieldException, IllegalAccessException {
        setDefaultMQProducerImpl();
        MessageQueue mq = mock(MessageQueue.class);
        long result = producer.searchOffset(mq, System.currentTimeMillis());
        assertEquals(0L, result);
    }

    @Test
    public void assertBatchMaxDelayMs() throws NoSuchFieldException, IllegalAccessException {
        setProduceAccumulator(true);
        assertEquals(0, producer.getBatchMaxDelayMs());
        setProduceAccumulator(false);
        assertEquals(10, producer.getBatchMaxDelayMs());
        producer.batchMaxDelayMs(1000);
        assertEquals(1000, producer.getBatchMaxDelayMs());
    }

    @Test
    public void assertBatchMaxBytes() throws NoSuchFieldException, IllegalAccessException {
        setProduceAccumulator(true);
        assertEquals(0L, producer.getBatchMaxBytes());
        setProduceAccumulator(false);
        assertEquals(32 * 1024L, producer.getBatchMaxBytes());
        producer.batchMaxBytes(64 * 1024L);
        assertEquals(64 * 1024L, producer.getBatchMaxBytes());
    }

    @Test
    public void assertTotalBatchMaxBytes() throws NoSuchFieldException, IllegalAccessException {
        setProduceAccumulator(true);
        assertEquals(0L, producer.getTotalBatchMaxBytes());
    }

    @Test
    public void assertGetRetryResponseCodes() {
        assertNotNull(producer.getRetryResponseCodes());
        assertEquals(7, producer.getRetryResponseCodes().size());
    }

    @Test
    public void assertIsSendLatencyFaultEnable() {
        assertFalse(producer.isSendLatencyFaultEnable());
    }

    @Test
    public void assertGetLatencyMax() {
        assertNotNull(producer.getLatencyMax());
    }

    @Test
    public void assertGetNotAvailableDuration() {
        assertNotNull(producer.getNotAvailableDuration());
    }

    @Test
    public void assertIsRetryAnotherBrokerWhenNotStoreOK() {
        assertFalse(producer.isRetryAnotherBrokerWhenNotStoreOK());
    }

    private void setOtherParam() {
        producer.setCreateTopicKey("createTopicKey");
        producer.setRetryAnotherBrokerWhenNotStoreOK(false);
        producer.setDefaultTopicQueueNums(6);
        producer.setRetryTimesWhenSendFailed(1);
        producer.setSendMessageWithVIPChannel(false);
        producer.setNotAvailableDuration(new long[1]);
        producer.setLatencyMax(new long[1]);
        producer.setSendLatencyFaultEnable(false);
        producer.setRetryTimesWhenSendAsyncFailed(1);
        producer.setTopics(Collections.singletonList(topic));
        producer.setStartDetectorEnable(false);
        producer.setCompressLevel(5);
        producer.setCompressType(CompressionType.LZ4);
        producer.addRetryResponseCode(0);
        ExecutorService executorService = mock(ExecutorService.class);
        producer.setAsyncSenderExecutor(executorService);
    }

    private void setProduceAccumulator(final boolean isDefault) throws NoSuchFieldException, IllegalAccessException {
        ProduceAccumulator accumulator = null;
        if (!isDefault) {
            accumulator = new ProduceAccumulator("instanceName");
        }
        setField(producer, "produceAccumulator", accumulator);
    }

    private void setDefaultMQProducerImpl() throws NoSuchFieldException, IllegalAccessException {
        DefaultMQProducerImpl producerImpl = mock(DefaultMQProducerImpl.class);
        setField(producer, "defaultMQProducerImpl", producerImpl);
        when(producerImpl.getMqFaultStrategy()).thenReturn(mock(MQFaultStrategy.class));
    }

    private void setField(final Object target, final String fieldName, final Object newValue) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = target.getClass();
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, newValue);
    }
}
