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

package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.AbstractLiteLifecycleManager;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
import org.apache.rocketmq.broker.longpolling.PopLiteLongPollingService;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.PopConsumerLockService;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class PopLiteMessageProcessorTest {

    @Mock
    private BrokerController brokerController;
    @Mock
    private MessageStore messageStore;
    @Mock
    private LiteEventDispatcher liteEventDispatcher;
    @Mock
    private PopLiteLongPollingService popLiteLongPollingService;
    @Mock
    private PopConsumerLockService lockService;
    @Mock
    private ConsumerOrderInfoManager consumerOrderInfoManager;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private PopMessageProcessor popMessageProcessor;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;
    @Mock
    private AbstractLiteLifecycleManager liteLifecycleManager;

    private BrokerConfig brokerConfig;
    private PopLiteMessageProcessor popLiteMessageProcessor;

    @Before
    public void setUp() throws Exception {
        brokerConfig = new BrokerConfig();
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getLiteLifecycleManager()).thenReturn(liteLifecycleManager);

        PopLiteMessageProcessor testObject = new PopLiteMessageProcessor(brokerController, liteEventDispatcher);
        FieldUtils.writeDeclaredField(testObject, "popLiteLongPollingService", popLiteLongPollingService, true);
        FieldUtils.writeDeclaredField(testObject, "lockService", lockService, true);
        FieldUtils.writeDeclaredField(testObject, "consumerOrderInfoManager", consumerOrderInfoManager, true);
        popLiteMessageProcessor = Mockito.spy(testObject);
    }

    @Test
    public void testRejectRequest() {
        assertFalse(popLiteMessageProcessor.rejectRequest());
    }

    @Test
    public void testTransformOrderCountInfo_empty() {
        StringBuilder result = popLiteMessageProcessor.transformOrderCountInfo(new StringBuilder(), 3);
        assertEquals("0;0;0", result.toString());
    }

    @Test
    public void testTransformOrderCountInfo_onlyQueueIdInfo() {
        StringBuilder input = new StringBuilder("0" + MessageConst.KEY_SEPARATOR + "0" + MessageConst.KEY_SEPARATOR + "2");
        StringBuilder result = popLiteMessageProcessor.transformOrderCountInfo(input, 3);
        assertEquals("2;2;2", result.toString());
    }

    @Test
    public void testTransformOrderCountInfo_consumeCountAndQueueIdInfo() {
        StringBuilder input = new StringBuilder("0 qo0%0 0;0 qo0%1 1;0 0 1");
        StringBuilder result = popLiteMessageProcessor.transformOrderCountInfo(input, 2);
        assertEquals("0 qo0%0 0;0 qo0%1 1", result.toString());
    }

    @Test
    public void testIsFifoBlocked() {
        when(consumerOrderInfoManager.checkBlock(anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(true);
        assertTrue(popLiteMessageProcessor.isFifoBlocked("attemptId", "group", "lmqName", 1000L));
        verify(consumerOrderInfoManager).checkBlock("attemptId", "lmqName", "group", 0, 1000L);
    }

    @Test
    public void testGetPopOffset_normal() throws ConsumeQueueException {
        String group = "group";
        String lmqName = "lmqName";
        long consumerOffset = 100L;

        // exist
        when(consumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(consumerOffset);
        when(consumerOffsetManager.queryThenEraseResetOffset(lmqName, group, 0)).thenReturn(null);
        assertEquals(consumerOffset, popLiteMessageProcessor.getPopOffset(group, lmqName));

        // not exist, init mode
        long initOffset = 10L;
        when(consumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(-1L);
        when(popMessageProcessor.getInitOffset(lmqName, group, 0, 1, true)).thenReturn(initOffset);

        assertEquals(initOffset, popLiteMessageProcessor.getPopOffset(group, lmqName));

        verify(consumerOffsetManager, times(2)).queryThenEraseResetOffset(lmqName, group, 0);
        verify(consumerOrderInfoManager, never()).clearBlock(anyString(), anyString(), anyInt());
        verify(consumerOffsetManager, never()).commitOffset(anyString(), anyString(), anyString(), anyInt(), anyLong());
    }


    @Test
    public void testGetPopOffset_resetOffset() {
        String group = "group";
        String lmqName = "lmq";
        long consumerOffset = 100L;
        long resetOffset = 50L;

        when(consumerOffsetManager.queryOffset(group, lmqName, 0)).thenReturn(consumerOffset);
        when(consumerOffsetManager.queryThenEraseResetOffset(lmqName, group, 0)).thenReturn(resetOffset);

        assertEquals(resetOffset, popLiteMessageProcessor.getPopOffset(group, lmqName));

        verify(consumerOffsetManager).queryOffset(group, lmqName, 0);
        verify(consumerOffsetManager).queryThenEraseResetOffset(lmqName, group, 0);
        verify(consumerOrderInfoManager).clearBlock(lmqName, group, 0);
        verify(consumerOffsetManager).commitOffset("ResetOffset", group, lmqName, 0, resetOffset);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPopByClientId_noEvent() {
        Iterator<String> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(false);
        when(liteEventDispatcher.getEventIterator("clientId")).thenReturn(mockIterator);

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popByClientId(
            "clientHost", "parentTopic", "group", "clientId", System.currentTimeMillis(), 6000L, 32, "attemptId");

        assertEquals(0, result.getObject1().length());
        assertEquals(0, result.getObject2().getMessageCount());
        verify(liteEventDispatcher).getEventIterator("clientId");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPopByClientId_oneEvent() {
        String event = "lmqName";
        int msgCount = 1;
        GetMessageResult mockResult = mockGetMessageResult(GetMessageStatus.FOUND, msgCount, 100L);
        long pollTime = System.currentTimeMillis();

        Iterator<String> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next()).thenReturn(event);
        when(liteEventDispatcher.getEventIterator("clientId")).thenReturn(mockIterator);
        doReturn(new Pair<>(new StringBuilder("0"), mockResult))
            .when(popLiteMessageProcessor)
            .popLiteTopic(anyString(), anyString(), anyString(), anyString(), anyLong(), anyLong(), anyLong(), anyString());

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popByClientId(
            "clientHost", "parentTopic", "group", "clientId", pollTime, 6000L, 32, "attemptId");

        assertEquals(msgCount, result.getObject2().getMessageCount());
        verify(mockIterator, times(2)).hasNext();
        verify(popLiteMessageProcessor).popLiteTopic("parentTopic" ,"clientHost", "group", event, 32L, pollTime, 6000L, "attemptId");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPopByClientId_resultFull() {
        String event1 = "lmqName1";
        String event2 = "lmqName2";
        int msgCount = 1;
        GetMessageResult mockResult = mockGetMessageResult(GetMessageStatus.FOUND, msgCount, 100L);
        long pollTime = System.currentTimeMillis();

        Iterator<String> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(true, true, true, true, false);
        when(mockIterator.next()).thenReturn(event1, event2, "event3", "event4");
        when(liteEventDispatcher.getEventIterator("clientId")).thenReturn(mockIterator);
        doReturn(new Pair<>(new StringBuilder("0"), mockResult))
            .when(popLiteMessageProcessor)
            .popLiteTopic(anyString(), anyString(), anyString(), anyString(), anyLong(), anyLong(), anyLong(), anyString());

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popByClientId(
            "clientHost", "parentTopic", "group", "clientId", pollTime, 6000L, 2, "attemptId");

        assertEquals(2, result.getObject2().getMessageCount());
        assertEquals("0;0", result.getObject1().toString());
        verify(mockIterator, times(2)).hasNext();
        verify(popLiteMessageProcessor).popLiteTopic("parentTopic", "clientHost", "group", event1, 2L, pollTime, 6000L, "attemptId");
        verify(popLiteMessageProcessor).popLiteTopic("parentTopic", "clientHost", "group", event2, 1L, pollTime, 6000L, "attemptId");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPopByClientId_duplicateEvent() {
        String event1 = "lmqName1";
        String event2 = "lmqName2";
        String event3 = "lmqName1";
        int msgCount = 1;
        GetMessageResult mockResult = mockGetMessageResult(GetMessageStatus.FOUND, msgCount, 100L);
        long pollTime = System.currentTimeMillis();

        Iterator<String> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        when(mockIterator.next()).thenReturn(event1, event2, event3);
        when(liteEventDispatcher.getEventIterator("clientId")).thenReturn(mockIterator);
        doReturn(new Pair<>(new StringBuilder("0"), mockResult))
            .when(popLiteMessageProcessor)
            .popLiteTopic(anyString(), anyString(), anyString(), anyString(), anyLong(), anyLong(), anyLong(), anyString());

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popByClientId(
            "clientHost", "parentTopic", "group", "clientId", pollTime, 6000L, 32, "attemptId");

        assertEquals(2, result.getObject2().getMessageCount());
        assertEquals("0;0", result.getObject1().toString());
        verify(mockIterator, times(4)).hasNext();
        verify(popLiteMessageProcessor).popLiteTopic("parentTopic", "clientHost", "group", event1, 32L, pollTime, 6000L, "attemptId");
        verify(popLiteMessageProcessor).popLiteTopic("parentTopic", "clientHost", "group", event2, 31L, pollTime, 6000L, "attemptId");
    }

    @Test
    public void testGetMessage_found() {
        String group = "group";
        String lmqName = "lmqName";
        String clientHost = "clientHost";
        long offset = 50L;
        int batchSize = 16;
        GetMessageResult mockResult = mockGetMessageResult(GetMessageStatus.FOUND, 1, 100L);
        when(messageStore.getMessage(group, lmqName, 0, offset, batchSize, null)).thenReturn(mockResult);

        GetMessageResult getMessageResult =
            popLiteMessageProcessor.getMessage(clientHost, group, lmqName, offset, batchSize);
        assertEquals(mockResult, getMessageResult);
        verify(consumerOffsetManager, never()).commitOffset(clientHost, group, lmqName, 0, 100L);
    }

    @Test
    public void testGetMessage_notFound() {
        String group = "group";
        String lmqName = "lmqName";
        String clientHost = "clientHost";
        long offset = 50L;
        long nextBeginOffset = 100L;
        int batchSize = 16;

        GetMessageResult firstResult = mockGetMessageResult(GetMessageStatus.MESSAGE_WAS_REMOVING, 0, nextBeginOffset);
        when(messageStore.getMessage(group, lmqName, 0, offset, batchSize, null)).thenReturn(firstResult);
        GetMessageResult secondResult = mockGetMessageResult(GetMessageStatus.FOUND, batchSize, nextBeginOffset + batchSize);
        when(messageStore.getMessage(group, lmqName, 0, nextBeginOffset, batchSize, null)).thenReturn(secondResult);

        GetMessageResult getMessageResult =
            popLiteMessageProcessor.getMessage(clientHost, group, lmqName, offset, batchSize);
        assertEquals(secondResult, getMessageResult);
        assertEquals(116, secondResult.getNextBeginOffset());
        verify(consumerOffsetManager).commitOffset("CorrectOffset", group, lmqName, 0, nextBeginOffset);
    }

    @Test
    public void testHandleGetMessageResult_nullResult() {
        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.handleGetMessageResult(
            null, "parentTopic", "group", "lmqName", System.currentTimeMillis(), 6000L, "attemptId");
        assertNull(result);
    }

    @Test
    public void testHandleGetMessageResult_found() {
        int msgCount = 2;
        GetMessageResult getResult = mockGetMessageResult(GetMessageStatus.FOUND, msgCount, 100L);
        getResult.getMessageQueueOffset().add(0L);
        getResult.getMessageQueueOffset().add(1L);

        doNothing().when(popLiteMessageProcessor).recordPopLiteMetrics(any(), anyString(), anyString());

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.handleGetMessageResult(
            getResult, "parentTopic", "group", "lmqName", System.currentTimeMillis(), 6000L, "attemptId");

        assertNotNull(result);
        assertEquals(getResult, result.getObject2());
        assertEquals("0;0", result.getObject1().toString());
    }

    @Test
    public void testPopLiteTopic_lockFailed() {
        when(lockService.tryLock(anyString())).thenReturn(false);

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popLiteTopic("parentTopic",
            "clientHost", "group", "lmqName", 32L, System.currentTimeMillis(), 6000L, "attemptId");

        assertNull(result);
        verify(lockService).tryLock(anyString());
        verify(lockService, never()).unlock(anyString());
    }

    @Test
    public void testPopLiteTopic_fifoBlocked() {
        when(lockService.tryLock(anyString())).thenReturn(true);
        when(consumerOrderInfoManager.checkBlock(anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(true);

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popLiteTopic("parentTopic",
            "clientHost", "group", "lmqName", 32L, System.currentTimeMillis(), 6000L, "attemptId");

        assertThat(result).isNull();
        verify(lockService).tryLock(anyString());
        verify(lockService).unlock(anyString());
    }

    @Test
    public void testPopLiteTopic_lmqNotExist() {
        when(liteLifecycleManager.isLmqExist("lmqName")).thenReturn(false);
        brokerConfig.setEnableLiteEventMode(false);

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popLiteTopic("parentTopic",
            "clientHost", "group", "lmqName", 32L, System.currentTimeMillis(), 6000L, "attemptId");

        assertThat(result).isNull();
        verify(lockService, never()).tryLock(anyString());
    }

    @Test
    public void testPopLiteTopic_found() {
        when(lockService.tryLock(anyString())).thenReturn(true);
        when(consumerOrderInfoManager.checkBlock(anyString(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(false);
        GetMessageResult mockResult = mockGetMessageResult(GetMessageStatus.FOUND, 1, 100L);
        when(messageStore.getMessage("group", "lmqName", 0, 0, 32, null)).thenReturn(mockResult);

        Pair<StringBuilder, GetMessageResult> result = popLiteMessageProcessor.popLiteTopic("parentTopic",
            "clientHost", "group", "lmqName", 32L, System.currentTimeMillis(), 6000L, "attemptId");

        assertEquals(mockResult, result.getObject2());
        verify(lockService).tryLock(anyString());
        verify(lockService).unlock(anyString());
    }

    @Test
    public void testPreCheck() {
        final String parentTopic = "parentTopic";
        final String group = "group";
        final TopicConfig topicConfig = new TopicConfig();
        final SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        RemotingCommand response = RemotingCommand.createResponseCommand(PopLiteMessageResponseHeader.class);
        PopLiteMessageRequestHeader requestHeader = new PopLiteMessageRequestHeader();
        when(topicConfigManager.selectTopicConfig(parentTopic)).thenReturn(topicConfig);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        // timeout too much
        requestHeader.setBornTime(System.currentTimeMillis() - 60000);
        requestHeader.setPollTime(30000);

        RemotingCommand result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertEquals(ResponseCode.POLLING_TIMEOUT, result.getCode());

        // not readable
        brokerConfig.setBrokerPermission(PermName.PERM_WRITE);
        requestHeader.setBornTime(System.currentTimeMillis());
        requestHeader.setPollTime(30000);

        result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertEquals(ResponseCode.NO_PERMISSION, result.getCode());
        brokerConfig.setBrokerPermission(PermName.PERM_READ | PermName.PERM_WRITE);

        // topic not exist
        requestHeader.setTopic("whatever");

        result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertEquals(ResponseCode.TOPIC_NOT_EXIST, result.getCode());

        // not lite topic type
        requestHeader.setTopic(parentTopic);

        result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertEquals(ResponseCode.INVALID_PARAMETER, result.getCode());

        // group not exist
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        topicConfig.setTopicMessageType(TopicMessageType.LITE);
        requestHeader.setConsumerGroup("whatever");

        result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertEquals(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, result.getCode());

        // group disable
        groupConfig.setConsumeEnable(false);
        requestHeader.setConsumerGroup(group);

        result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertEquals(ResponseCode.NO_PERMISSION, result.getCode());
        groupConfig.setConsumeEnable(true);

        // bind topic not match
        groupConfig.setLiteBindTopic("otherTopic");
        requestHeader.setMaxMsgNum(32);

        result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertEquals(ResponseCode.INVALID_PARAMETER, result.getCode());

        // normal
        groupConfig.setLiteBindTopic(parentTopic);
        result = popLiteMessageProcessor.preCheck(ctx, requestHeader, response);
        assertNull(result);
    }


    private GetMessageResult mockGetMessageResult(GetMessageStatus status, int messageCount, long nextBeginOffset) {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(status);
        getMessageResult.setMinOffset(0);
        getMessageResult.setMaxOffset(1024);
        getMessageResult.setNextBeginOffset(nextBeginOffset);

        if (GetMessageStatus.FOUND.equals(status)) {
            for (int i = 0; i < messageCount; i++) {
                getMessageResult.addMessage(Mockito.mock(SelectMappedBufferResult.class));
            }
        }
        return getMessageResult;
    }
}
