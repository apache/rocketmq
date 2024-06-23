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
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
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

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerImplTest {

    @Mock
    private Message message;

    @Mock
    private MessageQueue messageQueue;

    @Mock
    private MessageQueueSelector queueSelector;

    @Mock
    private RequestCallback requestCallback;

    @Mock
    private MQClientInstance mQClientFactory;

    private DefaultMQProducerImpl defaultMQProducerImpl;

    private final long defaultTimeout = 3000L;

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final String defaultTopic = "testTopic";

    @Before
    public void init() throws Exception {
        when(mQClientFactory.getTopicRouteTable()).thenReturn(mock(ConcurrentMap.class));
        when(mQClientFactory.getClientId()).thenReturn("client-id");
        when(mQClientFactory.getMQAdminImpl()).thenReturn(mock(MQAdminImpl.class));
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(messageQueue.getTopic()).thenReturn(defaultTopic);
        when(clientConfig.queueWithNamespace(any())).thenReturn(messageQueue);
        when(mQClientFactory.getClientConfig()).thenReturn(clientConfig);
        when(mQClientFactory.getTopicRouteTable()).thenReturn(mock(ConcurrentMap.class));
        MQClientAPIImpl mQClientAPIImpl = mock(MQClientAPIImpl.class);
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mQClientAPIImpl);
        when(mQClientFactory.findBrokerAddressInPublish(or(isNull(), anyString()))).thenReturn(defaultBrokerAddr);
        when(message.getTopic()).thenReturn(defaultTopic);
        when(message.getProperty(MessageConst.PROPERTY_CORRELATION_ID)).thenReturn("correlation-id");
        when(message.getBody()).thenReturn(new byte[1]);
        TransactionMQProducer producer = new TransactionMQProducer("test-producer-group");
        producer.setTransactionListener(mock(TransactionListener.class));
        producer.setTopics(Collections.singletonList(defaultTopic));
        defaultMQProducerImpl = new DefaultMQProducerImpl(producer);
        setMQClientFactory();
        setCheckExecutor();
        setCheckForbiddenHookList();
        setTopicPublishInfoTable();
        defaultMQProducerImpl.setServiceState(ServiceState.RUNNING);
    }

    @Test
    public void testRequest() throws Exception {
        defaultMQProducerImpl.request(message, messageQueue, requestCallback, defaultTimeout);
        defaultMQProducerImpl.request(message, queueSelector, 1, requestCallback, defaultTimeout);
    }

    @Test(expected = MQClientException.class)
    public void testRequestMQClientExceptionByVoid() throws Exception {
        defaultMQProducerImpl.request(message, requestCallback, defaultTimeout);
    }

    @Test
    public void testCheckTransactionState() {
        defaultMQProducerImpl.checkTransactionState(defaultBrokerAddr, mock(MessageExt.class), mock(CheckTransactionStateRequestHeader.class));
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
    public void testSendOneway() throws MQClientException, InterruptedException, RemotingException, NoSuchFieldException, IllegalAccessException {
        defaultMQProducerImpl.sendOneway(message);
    }

    @Test
    public void testSendOnewayByQueueSelector() throws MQClientException, InterruptedException, RemotingException, NoSuchFieldException, IllegalAccessException {
        defaultMQProducerImpl.sendOneway(message, mock(MessageQueueSelector.class), 1);
    }

    @Test
    public void testSendOnewayByQueue() throws MQClientException, InterruptedException, RemotingException, NoSuchFieldException, IllegalAccessException {
        defaultMQProducerImpl.sendOneway(message, messageQueue);
    }

    @Test(expected = MQClientException.class)
    public void testSend() throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        assertNull(defaultMQProducerImpl.send(message));
    }

    @Test
    public void assertSendByQueue() throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        SendResult actual = defaultMQProducerImpl.send(message, messageQueue);
        assertNull(actual);
        actual = defaultMQProducerImpl.send(message, messageQueue, defaultTimeout);
        assertNull(actual);
    }

    @Test
    public void assertSendByQueueSelector() throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        SendCallback sendCallback = mock(SendCallback.class);
        defaultMQProducerImpl.send(message, queueSelector, 1, sendCallback);
        SendResult actual = defaultMQProducerImpl.send(message, queueSelector, 1);
        assertNull(actual);
        actual = defaultMQProducerImpl.send(message, queueSelector, 1, defaultTimeout);
        assertNull(actual);
    }

    @Test(expected = MQClientException.class)
    public void assertMQClientException() throws Exception {
        assertNull(defaultMQProducerImpl.request(message, defaultTimeout));
    }

    @Test(expected = RequestTimeoutException.class)
    public void assertRequestRequestTimeoutByQueueSelector() throws Exception {
        assertNull(defaultMQProducerImpl.request(message, queueSelector, 1, defaultTimeout));
    }

    @Test(expected = Exception.class)
    public void assertRequestTimeoutExceptionByQueue() throws Exception {
        assertNull(defaultMQProducerImpl.request(message, messageQueue, defaultTimeout));
    }

    @Test
    public void testRegisterCheckForbiddenHook() {
        CheckForbiddenHook checkForbiddenHook = mock(CheckForbiddenHook.class);
        defaultMQProducerImpl.registerCheckForbiddenHook(checkForbiddenHook);
    }

    @Test
    public void testInitTopicRoute() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> clazz = defaultMQProducerImpl.getClass();
        Method method = clazz.getDeclaredMethod("initTopicRoute");
        method.setAccessible(true);
        method.invoke(defaultMQProducerImpl);
    }

    @Test
    public void assertFetchPublishMessageQueues() throws MQClientException {
        List<MessageQueue> actual = defaultMQProducerImpl.fetchPublishMessageQueues(defaultTopic);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void assertSearchOffset() throws MQClientException {
        assertEquals(0, defaultMQProducerImpl.searchOffset(messageQueue, System.currentTimeMillis()));
    }

    @Test
    public void assertMaxOffset() throws MQClientException {
        assertEquals(0, defaultMQProducerImpl.maxOffset(messageQueue));
    }

    @Test
    public void assertMinOffset() throws MQClientException {
        assertEquals(0, defaultMQProducerImpl.minOffset(messageQueue));
    }

    @Test
    public void assertEarliestMsgStoreTime() throws MQClientException {
        assertEquals(0, defaultMQProducerImpl.earliestMsgStoreTime(messageQueue));
    }

    @Test
    public void assertViewMessage() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        assertNull(defaultMQProducerImpl.viewMessage(defaultTopic, "msgId"));
    }

    @Test
    public void assertQueryMessage() throws MQClientException, InterruptedException {
        assertNull(defaultMQProducerImpl.queryMessage(defaultTopic, "key", 1, 0L, 10L));
    }

    @Test
    public void assertQueryMessageByUniqKey() throws MQClientException, InterruptedException {
        assertNull(defaultMQProducerImpl.queryMessageByUniqKey(defaultTopic, "key"));
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
        assertNotNull(defaultMQProducerImpl.getMqFaultStrategy());
    }

    @Test
    public void assertCheckListener() {
        assertNull(defaultMQProducerImpl.checkListener());
    }

    private void setMQClientFactory() throws IllegalAccessException, NoSuchFieldException {
        setField(defaultMQProducerImpl, "mQClientFactory", mQClientFactory);
    }

    private void setTopicPublishInfoTable() throws IllegalAccessException, NoSuchFieldException {
        ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
        TopicPublishInfo topicPublishInfo = mock(TopicPublishInfo.class);
        when(topicPublishInfo.ok()).thenReturn(true);
        topicPublishInfoTable.put(defaultTopic, topicPublishInfo);
        setField(defaultMQProducerImpl, "topicPublishInfoTable", topicPublishInfoTable);
    }

    private void setCheckExecutor() throws NoSuchFieldException, IllegalAccessException {
        setField(defaultMQProducerImpl, "checkExecutor", mock(ExecutorService.class));
    }

    private void setCheckForbiddenHookList() throws NoSuchFieldException, IllegalAccessException {
        ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<>();
        checkForbiddenHookList.add(mock(CheckForbiddenHook.class));
        setField(defaultMQProducerImpl, "checkForbiddenHookList", checkForbiddenHookList);
    }

    private void setField(final Object target, final String fieldName, final Object newValue) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = target.getClass();
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, newValue);
    }
}
