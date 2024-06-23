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

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.impl.MQAdminImpl;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerImplTest {

    @Mock
    private Message message;

    @Mock
    private MessageQueue messageQueue;

    @Mock
    private RequestCallback requestCallback;

    @Mock
    private MQClientInstance mQClientFactory;

    @Mock
    private SendCallback sendCallback;

    private DefaultMQProducerImpl defaultMQProducerImpl;

    private final long defaultTimeout = 30000L;

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final String defaultTopic = "testTopic";

    @Before
    public void init() throws Exception {
        when(mQClientFactory.getTopicRouteTable()).thenReturn(mock(ConcurrentMap.class));
        when(mQClientFactory.getClientId()).thenReturn("client-id");
        when(mQClientFactory.getMQAdminImpl()).thenReturn(mock(MQAdminImpl.class));
        when(message.getTopic()).thenReturn(defaultTopic);
        TransactionMQProducer producer = new TransactionMQProducer("test-producer-group");
        producer.setTransactionListener(mock(TransactionListener.class));
        producer.setTopics(Collections.singletonList(defaultTopic));
        defaultMQProducerImpl = new DefaultMQProducerImpl(producer);
        setTopicPublishInfoTable(false);
        setMQClientFactory();
        setCheckExecutor();
        defaultMQProducerImpl.setServiceState(ServiceState.RUNNING);
    }

    @Test(expected = ExecutionException.class)
    public void testRequest() throws Exception {
        setTopicPublishInfoTable(true);
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        List<Callable<Message>> callables = Arrays.asList(() -> {
            defaultMQProducerImpl.request(message, messageQueue, requestCallback, defaultTimeout);
            return null;
        }, () -> defaultMQProducerImpl.request(message, messageQueue, defaultTimeout), () -> {
            defaultMQProducerImpl.request(message, selector, 1, requestCallback, defaultTimeout);
            return null;
        }, () -> defaultMQProducerImpl.request(message, selector, 1, defaultTimeout));
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        assertNull(executorService.invokeAny(callables));
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
    public void testSendOneway() throws MQClientException, RemotingException, InterruptedException {
        defaultMQProducerImpl.sendOneway(message);
    }

    @Test(expected = MQClientException.class)
    public void testSendOnewayByQueueSelector() throws MQClientException, RemotingException, InterruptedException {
        defaultMQProducerImpl.sendOneway(message, mock(MessageQueueSelector.class), 1);
    }

    @Test
    public void assertSend() throws InterruptedException, ExecutionException {
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        List<Callable<SendResult>> callables = Arrays.asList(
                () -> defaultMQProducerImpl.send(message),
                () -> defaultMQProducerImpl.send(message, selector, 1),
                () -> defaultMQProducerImpl.send(message, selector, 1, defaultTimeout),
                () -> defaultMQProducerImpl.send(message, mock(MessageQueue.class), defaultTimeout),
                () -> {
                    defaultMQProducerImpl.send(message, selector, 1, sendCallback);
                    return null;
                });
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        assertNull(executorService.invokeAny(callables));
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

    private void setMQClientFactory() throws IllegalAccessException, NoSuchFieldException {
        setField(defaultMQProducerImpl, "mQClientFactory", mQClientFactory);
    }

    private void setTopicPublishInfoTable(final boolean isOk) throws IllegalAccessException, NoSuchFieldException {
        ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
        TopicPublishInfo topicPublishInfo = mock(TopicPublishInfo.class);
        when(topicPublishInfo.ok()).thenReturn(isOk);
        topicPublishInfoTable.put(defaultTopic, topicPublishInfo);
        setField(defaultMQProducerImpl, "topicPublishInfoTable", topicPublishInfoTable);
    }

    private void setCheckExecutor() throws NoSuchFieldException, IllegalAccessException {
        setField(defaultMQProducerImpl, "checkExecutor", mock(ExecutorService.class));
    }

    private void setField(final Object target, final String fieldName, final Object newValue) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = target.getClass();
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, newValue);
    }
}
