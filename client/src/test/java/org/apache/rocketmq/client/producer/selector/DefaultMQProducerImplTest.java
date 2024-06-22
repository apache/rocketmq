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
package org.apache.rocketmq.client.producer.selector;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.support.membermodification.MemberMatcher.field;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Validators.class, MQClientManager.class, RequestFutureHolder.class})
public class DefaultMQProducerImplTest {

    @Mock
    private Message message;

    @Mock
    private MessageQueue messageQueue;

    @Mock
    private RequestCallback requestCallback;

    @Mock
    private DefaultMQProducer defaultMQProducer;

    @Mock
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientManager mqClientManager;

    @Mock
    private RequestFutureHolder requestResponseFuture;

    @Mock
    private ClientConfig clientConfig;

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    @Mock
    private SendCallback sendCallback;

    private DefaultMQProducerImpl defaultMQProducerImpl;

    private final long defaultTimeout = 30000L;

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final String defaultNsAddr = "127.0.0.1:9876";

    private final String defaultTopic = "testTopic";

    @Before
    public void init() throws Exception {
        mockStatic(Validators.class);
        doNothing().when(Validators.class, "checkGroup", anyString());
        mockStatic(MQClientManager.class);
        mockStatic(RequestFutureHolder.class);
        when(RequestFutureHolder.getInstance()).thenReturn(requestResponseFuture);
        ConcurrentHashMap requestFutureTable = mock(ConcurrentHashMap.class);
        when(RequestFutureHolder.getInstance().getRequestFutureTable()).thenReturn(requestFutureTable);
        when(MQClientManager.getInstance()).thenReturn(mqClientManager);
        when(MQClientManager.getInstance().getOrCreateMQClientInstance(any(), any())).thenReturn(mQClientFactory);
        when(mQClientFactory.registerProducer(anyString(), any())).thenReturn(true);
        when(mQClientFactory.getTopicRouteTable()).thenReturn(mock(ConcurrentMap.class));
        when(mQClientFactory.findBrokerAddressInPublish(or(isNull(), anyString()))).thenReturn(defaultBrokerAddr);
        when(clientConfig.queueWithNamespace(any())).thenReturn(messageQueue);
        when(mQClientFactory.getClientConfig()).thenReturn(clientConfig);
        when(mQClientFactory.getClientId()).thenReturn("client-id");
        when(mQClientFactory.getMQAdminImpl()).thenReturn(mock(MQAdminImpl.class));
        when(clientConfig.getNamespace()).thenReturn("namespace");
        when(message.getTopic()).thenReturn(defaultTopic);
        List<String> nsAddrList = Collections.singletonList(defaultNsAddr);
        when(mQClientAPIImpl.getNameServerAddressList()).thenReturn(nsAddrList);
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mQClientAPIImpl);
        when(defaultMQProducer.cloneClientConfig()).thenReturn(clientConfig);
        TransactionMQProducer producer = new TransactionMQProducer("test-producer-group");
        producer.setTransactionListener(mock(TransactionListener.class));
        defaultMQProducerImpl = producer.getDefaultMQProducerImpl();
        setTopicPublishInfoTable(defaultMQProducerImpl);
        setMQClientFactory(defaultMQProducerImpl);
        producer.start();
    }

    @Test(expected = RequestTimeoutException.class)
    public void testRequest() throws Exception {
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        defaultMQProducerImpl.request(message, messageQueue, requestCallback, defaultTimeout);
        defaultMQProducerImpl.request(message, selector, 1, requestCallback, defaultTimeout);
        defaultMQProducerImpl.request(message, messageQueue, requestCallback, defaultTimeout);
        Message actual = defaultMQProducerImpl.request(message, messageQueue, defaultTimeout);
        assertNull(actual);
    }

    @Test
    public void testCheckTransactionState() {
        MessageExt msg = mock(MessageExt.class);
        CheckTransactionStateRequestHeader header = mock(CheckTransactionStateRequestHeader.class);
        defaultMQProducerImpl.checkTransactionState(defaultBrokerAddr, msg, header);
    }

    @Test
    public void testCreateTopic() throws MQClientException {
        defaultMQProducerImpl.createTopic("key", defaultTopic, 0);
    }

    @Test
    public void testExecuteCheckForbiddenHook() throws MQClientException {
        defaultMQProducerImpl.executeCheckForbiddenHook(mock(CheckForbiddenContext.class));
    }

    @Test(expected = MQClientException.class)
    public void testSendOneway() throws MQClientException, RemotingException, InterruptedException {
        Message msg = mock(Message.class);
        when(msg.getTopic()).thenReturn(defaultTopic);
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        defaultMQProducerImpl.sendOneway(msg);
        defaultMQProducerImpl.sendOneway(msg, selector, 1);
    }

    @Test(expected = MQClientException.class)
    public void assertSend() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        Message msg = mock(Message.class);
        when(msg.getTopic()).thenReturn(defaultTopic);
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        SendResult actual = defaultMQProducerImpl.send(msg);
        assertNull(actual);
        actual = defaultMQProducerImpl.send(msg, selector, 1);
        assertNull(actual);
        actual = defaultMQProducerImpl.send(msg, selector, 1, defaultTimeout);
        assertNull(actual);
        defaultMQProducerImpl.send(msg, selector, 1, sendCallback);
        actual = defaultMQProducerImpl.send(msg, mock(MessageQueue.class), defaultTimeout);
        assertNull(actual);
    }

    @Test
    public void assertFetchPublishMessageQueues() throws MQClientException {
        List<MessageQueue> actual = defaultMQProducerImpl.fetchPublishMessageQueues(defaultTopic);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void assertSearchOffset() throws MQClientException {
        long actual = defaultMQProducerImpl.searchOffset(messageQueue, System.currentTimeMillis());
        assertEquals(0, actual);
    }

    @Test
    public void assertMaxOffset() throws MQClientException {
        long actual = defaultMQProducerImpl.maxOffset(messageQueue);
        assertEquals(0, actual);
    }

    @Test
    public void assertMinOffset() throws MQClientException {
        long actual = defaultMQProducerImpl.minOffset(messageQueue);
        assertEquals(0, actual);
    }

    @Test
    public void assertEarliestMsgStoreTime() throws MQClientException {
        long actual = defaultMQProducerImpl.earliestMsgStoreTime(messageQueue);
        assertEquals(0, actual);
    }

    @Test
    public void assertViewMessage() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        MessageExt actual = defaultMQProducerImpl.viewMessage(defaultTopic, "msgId");
        assertNull(actual);
    }

    @Test
    public void assertQueryMessage() throws MQClientException, InterruptedException {
        QueryResult actual = defaultMQProducerImpl.queryMessage(defaultTopic, "key", 1, 0L, 10L);
        assertNull(actual);
    }

    @Test
    public void assertQueryMessageByUniqKey() throws MQClientException, InterruptedException {
        MessageExt actual = defaultMQProducerImpl.queryMessageByUniqKey(defaultTopic, "key");
        assertNull(actual);
    }

    @Test
    public void assertSetAsyncSenderExecutor() {
        ExecutorService asyncSenderExecutor = mock(ExecutorService.class);
        defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
        assertEquals(asyncSenderExecutor, defaultMQProducerImpl.getAsyncSenderExecutor());
    }

    @Test
    public void assertServiceState() {
        ServiceState serviceState = defaultMQProducerImpl.getServiceState();
        assertNotNull(serviceState);
        assertEquals(ServiceState.RUNNING, serviceState);
        defaultMQProducerImpl.setServiceState(ServiceState.SHUTDOWN_ALREADY);
        serviceState = defaultMQProducerImpl.getServiceState();
        assertNotNull(serviceState);
        assertEquals(ServiceState.SHUTDOWN_ALREADY, serviceState);
    }

    @Test
    public void assertGetNotAvailableDuration() {
        long[] notAvailableDuration = defaultMQProducerImpl.getNotAvailableDuration();
        assertNotNull(notAvailableDuration);
        defaultMQProducerImpl.setNotAvailableDuration(new long[1]);
        notAvailableDuration = defaultMQProducerImpl.getNotAvailableDuration();
        assertNotNull(notAvailableDuration);
        assertEquals(1, notAvailableDuration.length);
    }

    @Test
    public void assertGetLatencyMax() {
        long[] actual = defaultMQProducerImpl.getLatencyMax();
        assertNotNull(actual);
        defaultMQProducerImpl.setLatencyMax(new long[1]);
        actual = defaultMQProducerImpl.getLatencyMax();
        assertNotNull(actual);
        assertEquals(1, actual.length);
    }

    @Test
    public void assertIsSendLatencyFaultEnable() {
        boolean actual = defaultMQProducerImpl.isSendLatencyFaultEnable();
        assertFalse(actual);
        defaultMQProducerImpl.setSendLatencyFaultEnable(true);
        actual = defaultMQProducerImpl.isSendLatencyFaultEnable();
        assertTrue(actual);
    }

    @Test
    public void assertGetMqFaultStrategy() {
        MQFaultStrategy actual = defaultMQProducerImpl.getMqFaultStrategy();
        assertNotNull(actual);
    }

    private void setMQClientFactory(final DefaultMQProducerImpl defaultMQProducerImpl) throws IllegalAccessException {
        field(DefaultMQProducerImpl.class, "mQClientFactory").set(defaultMQProducerImpl, mQClientFactory);
    }

    private void setTopicPublishInfoTable(final DefaultMQProducerImpl defaultMQProducerImpl) throws IllegalAccessException {
        ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
        TopicPublishInfo topicPublishInfo = mock(TopicPublishInfo.class);
        when(topicPublishInfo.ok()).thenReturn(true);
        topicPublishInfoTable.put(defaultTopic, topicPublishInfo);
        field(DefaultMQProducerImpl.class, "topicPublishInfoTable").set(defaultMQProducerImpl, topicPublishInfoTable);
    }
}
