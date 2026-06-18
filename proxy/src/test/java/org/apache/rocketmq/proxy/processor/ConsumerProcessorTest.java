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

package org.apache.rocketmq.proxy.processor;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerProcessorTest extends BaseProcessorTest {

    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String TOPIC = "topic";
    private static final String CLIENT_ID = "clientId";

    private ConsumerProcessor consumerProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        this.consumerProcessor = new ConsumerProcessor(messagingProcessor, serviceManager, Executors.newCachedThreadPool());
    }

    @Test
    public void testPopMessage() throws Throwable {
        final String tag = "tag";
        final long invisibleTime = Duration.ofSeconds(15).toMillis();
        ArgumentCaptor<AddressableMessageQueue> messageQueueArgumentCaptor = ArgumentCaptor.forClass(AddressableMessageQueue.class);
        ArgumentCaptor<PopMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(PopMessageRequestHeader.class);

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, "noMatch", 0, invisibleTime));
        messageExtList.add(createMessageExt(TOPIC, tag, 0, invisibleTime));
        messageExtList.add(createMessageExt(TOPIC, tag, 1, invisibleTime));
        PopResult innerPopResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(this.messageService.popMessage(any(), messageQueueArgumentCaptor.capture(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerPopResult));

        when(this.topicRouteService.getCurrentMessageQueueView(any(), anyString()))
            .thenReturn(mock(MessageQueueView.class));

        ArgumentCaptor<String> ackMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        when(this.messagingProcessor.ackMessage(any(), any(), ackMessageIdArgumentCaptor.capture(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        ArgumentCaptor<String> toDLQMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        when(this.messagingProcessor.forwardMessageToDeadLetterQueue(any(), any(), toDLQMessageIdArgumentCaptor.capture(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(RemotingCommand.class)));

        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        PopResult popResult = this.consumerProcessor.popMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            CONSUMER_GROUP,
            TOPIC,
            60,
            invisibleTime,
            Duration.ofSeconds(3).toMillis(),
            ConsumeInitMode.MAX,
            FilterAPI.build(TOPIC, tag, ExpressionType.TAG),
            false,
            (ctx, consumerGroup, subscriptionData, messageExt) -> {
                if (!messageExt.getTags().equals(tag)) {
                    return PopMessageResultFilter.FilterResult.NO_MATCH;
                }
                if (messageExt.getReconsumeTimes() > 0) {
                    return PopMessageResultFilter.FilterResult.TO_DLQ;
                }
                return PopMessageResultFilter.FilterResult.MATCH;
            },
            null,
            Duration.ofSeconds(3).toMillis()
        ).get();

        assertSame(messageQueue, messageQueueArgumentCaptor.getValue());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(TOPIC, requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, requestHeaderArgumentCaptor.getValue().getMaxMsgNums());
        assertEquals(tag, requestHeaderArgumentCaptor.getValue().getExp());
        assertEquals(ExpressionType.TAG, requestHeaderArgumentCaptor.getValue().getExpType());

        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(1, popResult.getMsgFoundList().size());
        assertEquals(messageExtList.get(1), popResult.getMsgFoundList().get(0));

        assertEquals(messageExtList.get(0).getMsgId(), ackMessageIdArgumentCaptor.getValue());
        assertEquals(messageExtList.get(2).getMsgId(), toDLQMessageIdArgumentCaptor.getValue());
    }

    @Test
    public void testAckMessage() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<AckMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(AckMessageRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.ackMessage(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.ackMessage(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, null, 3000).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
    }

    @Test
    public void testBatchAckExpireMessage() throws Throwable {
        String brokerName1 = "brokerName1";

        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MessageExt expireMessage = createMessageExt(TOPIC, "", 0, 3000, System.currentTimeMillis() - 10000,
                0, 0, 0, i, brokerName1);
            ReceiptHandle expireHandle = create(expireMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(expireHandle, expireMessage.getMsgId()));
        }

        List<BatchAckResult> batchAckResultList = this.consumerProcessor.batchAckMessage(createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000).get();

        verify(this.messageService, never()).batchAckMessage(any(), anyList(), anyString(), anyString(), anyLong());
        assertEquals(receiptHandleMessageList.size(), batchAckResultList.size());
        for (BatchAckResult batchAckResult : batchAckResultList) {
            assertNull(batchAckResult.getAckResult());
            assertNotNull(batchAckResult.getProxyException());
            assertNotNull(batchAckResult.getReceiptHandleMessage());
        }

    }

    @Test
    public void testBatchAckMessage() throws Throwable {
        String brokerName1 = "brokerName1";
        String brokerName2 = "brokerName2";
        String errThrowBrokerName = "errThrowBrokerName";
        MessageExt expireMessage = createMessageExt(TOPIC, "", 0, 3000, System.currentTimeMillis() - 10000,
            0, 0, 0, 0, brokerName1);
        ReceiptHandle expireHandle = create(expireMessage);

        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        receiptHandleMessageList.add(new ReceiptHandleMessage(expireHandle, expireMessage.getMsgId()));
        List<String> broker1Msg = new ArrayList<>();
        List<String> broker2Msg = new ArrayList<>();

        long now = System.currentTimeMillis();
        int msgNum = 3;
        for (int i = 0; i < msgNum; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName1);
            ReceiptHandle brokerHandle = create(brokerMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(brokerHandle, brokerMessage.getMsgId()));
            broker1Msg.add(brokerMessage.getMsgId());
        }
        for (int i = 0; i < msgNum; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName2);
            ReceiptHandle brokerHandle = create(brokerMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(brokerHandle, brokerMessage.getMsgId()));
            broker2Msg.add(brokerMessage.getMsgId());
        }

        // for this message, will throw exception in batchAckMessage
        MessageExt errThrowMessage = createMessageExt(TOPIC, "", 0, 3000, now,
            0, 0, 0, 0, errThrowBrokerName);
        ReceiptHandle errThrowHandle = create(errThrowMessage);
        receiptHandleMessageList.add(new ReceiptHandleMessage(errThrowHandle, errThrowMessage.getMsgId()));

        Collections.shuffle(receiptHandleMessageList);

        doAnswer((Answer<CompletableFuture<AckResult>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            AckResult ackResult = new AckResult();
            String brokerName = handleMessageList.get(0).getReceiptHandle().getBrokerName();
            if (brokerName.equals(brokerName1)) {
                ackResult.setStatus(AckStatus.OK);
            } else if (brokerName.equals(brokerName2)) {
                ackResult.setStatus(AckStatus.NO_EXIST);
            } else {
                return FutureUtils.completeExceptionally(new RuntimeException());
            }

            return CompletableFuture.completedFuture(ackResult);
        }).when(this.messageService).batchAckMessage(any(), anyList(), anyString(), anyString(), anyLong());

        List<BatchAckResult> batchAckResultList = this.consumerProcessor.batchAckMessage(createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000).get();
        assertEquals(receiptHandleMessageList.size(), batchAckResultList.size());

        // check ackResult for each msg
        Map<String, BatchAckResult> msgBatchAckResult = new HashMap<>();
        for (BatchAckResult batchAckResult : batchAckResultList) {
            msgBatchAckResult.put(batchAckResult.getReceiptHandleMessage().getMessageId(), batchAckResult);
        }
        for (String msgId : broker1Msg) {
            assertEquals(AckStatus.OK, msgBatchAckResult.get(msgId).getAckResult().getStatus());
            assertNull(msgBatchAckResult.get(msgId).getProxyException());
        }
        for (String msgId : broker2Msg) {
            assertEquals(AckStatus.NO_EXIST, msgBatchAckResult.get(msgId).getAckResult().getStatus());
            assertNull(msgBatchAckResult.get(msgId).getProxyException());
        }
        assertNotNull(msgBatchAckResult.get(expireMessage.getMsgId()).getProxyException());
        assertEquals(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, msgBatchAckResult.get(expireMessage.getMsgId()).getProxyException().getCode());
        assertNull(msgBatchAckResult.get(expireMessage.getMsgId()).getAckResult());

        assertNotNull(msgBatchAckResult.get(errThrowMessage.getMsgId()).getProxyException());
        assertEquals(ProxyExceptionCode.INTERNAL_SERVER_ERROR, msgBatchAckResult.get(errThrowMessage.getMsgId()).getProxyException().getCode());
        assertNull(msgBatchAckResult.get(errThrowMessage.getMsgId()).getAckResult());
    }

    @Test
    public void testBatchChangeInvisibleTime() throws Throwable {
        String brokerName1 = "brokerName1";
        String brokerName2 = "brokerName2";
        MessageExt expireMessage = createMessageExt(TOPIC, "", 0, 3000, System.currentTimeMillis() - 10000,
            0, 0, 0, 0, brokerName1);
        ReceiptHandle expireHandle = create(expireMessage);

        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        receiptHandleMessageList.add(new ReceiptHandleMessage(expireHandle, expireMessage.getMsgId()));
        List<String> broker1Msg = new ArrayList<>();
        List<String> broker2Msg = new ArrayList<>();

        long now = System.currentTimeMillis();
        int msgNum = 3;
        for (int i = 0; i < msgNum; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName1);
            ReceiptHandle brokerHandle = create(brokerMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(brokerHandle, brokerMessage.getMsgId()));
            broker1Msg.add(brokerMessage.getMsgId());
        }
        for (int i = 0; i < msgNum; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName2);
            ReceiptHandle brokerHandle = create(brokerMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(brokerHandle, brokerMessage.getMsgId()));
            broker2Msg.add(brokerMessage.getMsgId());
        }

        String newExtraInfo = "newExtraInfo";
        long popTime = 12345L;
        doAnswer((Answer<CompletableFuture<List<AckResult>>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            List<AckResult> ackResultList = new ArrayList<>();
            String brokerName = handleMessageList.get(0).getReceiptHandle().getBrokerName();
            for (ReceiptHandleMessage ignored : handleMessageList) {
                AckResult ackResult = new AckResult();
                if (brokerName.equals(brokerName1)) {
                    ackResult.setStatus(AckStatus.OK);
                    ackResult.setPopTime(popTime);
                    ackResult.setExtraInfo(newExtraInfo);
                } else {
                    ackResult.setStatus(AckStatus.NO_EXIST);
                }
                ackResultList.add(ackResult);
            }
            return CompletableFuture.completedFuture(ackResultList);
        }).when(this.messageService).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());

        List<BatchChangeInvisibleTimeResult> resultList = this.consumerProcessor.batchChangeInvisibleTime(
            createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000, 3000, true).get();

        assertEquals(receiptHandleMessageList.size(), resultList.size());
        Map<String, BatchChangeInvisibleTimeResult> msgResult = new HashMap<>();
        for (BatchChangeInvisibleTimeResult result : resultList) {
            msgResult.put(result.getReceiptHandleMessage().getMessageId(), result);
        }
        for (String msgId : broker1Msg) {
            BatchChangeInvisibleTimeResult result = msgResult.get(msgId);
            assertEquals(AckStatus.OK, result.getAckResult().getStatus());
            assertEquals(popTime, result.getAckResult().getPopTime());
            assertEquals(newExtraInfo + MessageConst.KEY_SEPARATOR
                + result.getReceiptHandleMessage().getReceiptHandle().getCommitLogOffset(),
                result.getAckResult().getExtraInfo());
            assertNull(result.getProxyException());
        }
        for (String msgId : broker2Msg) {
            assertEquals(AckStatus.NO_EXIST, msgResult.get(msgId).getAckResult().getStatus());
            assertNull(msgResult.get(msgId).getProxyException());
        }
        assertNotNull(msgResult.get(expireMessage.getMsgId()).getProxyException());
        assertEquals(ProxyExceptionCode.INVALID_RECEIPT_HANDLE,
            msgResult.get(expireMessage.getMsgId()).getProxyException().getCode());
        assertNull(msgResult.get(expireMessage.getMsgId()).getAckResult());
    }

    @Test
    public void testBatchChangeInvisibleTimePreserveInputOrderWithExpiredAndInterleavedGroups() throws Throwable {
        String brokerName1 = "brokerName1";
        String brokerName2 = "brokerName2";
        long now = System.currentTimeMillis();

        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        MessageExt broker2Message1 = createMessageExt(TOPIC, "", 0, 3000, now,
            0, 0, 0, 1, brokerName2);
        receiptHandleMessageList.add(new ReceiptHandleMessage(create(broker2Message1), broker2Message1.getMsgId()));

        MessageExt expireMessage = createMessageExt(TOPIC, "", 0, 3000, now - 10000,
            0, 0, 0, 2, brokerName1);
        receiptHandleMessageList.add(new ReceiptHandleMessage(create(expireMessage), expireMessage.getMsgId()));

        MessageExt broker1Message1 = createMessageExt(TOPIC, "", 0, 3000, now,
            0, 0, 0, 3, brokerName1);
        receiptHandleMessageList.add(new ReceiptHandleMessage(create(broker1Message1), broker1Message1.getMsgId()));

        MessageExt broker2Message2 = createMessageExt(TOPIC, "", 0, 3000, now,
            0, 0, 0, 4, brokerName2);
        receiptHandleMessageList.add(new ReceiptHandleMessage(create(broker2Message2), broker2Message2.getMsgId()));

        MessageExt broker1Message2 = createMessageExt(TOPIC, "", 0, 3000, now,
            0, 0, 0, 5, brokerName1);
        receiptHandleMessageList.add(new ReceiptHandleMessage(create(broker1Message2), broker1Message2.getMsgId()));

        doAnswer((Answer<CompletableFuture<List<AckResult>>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            List<AckResult> ackResultList = new ArrayList<>();
            for (ReceiptHandleMessage handleMessage : handleMessageList) {
                AckResult ackResult = new AckResult();
                ackResult.setStatus(AckStatus.OK);
                ackResult.setExtraInfo("extra-" + handleMessage.getMessageId());
                ackResultList.add(ackResult);
            }
            return CompletableFuture.completedFuture(ackResultList);
        }).when(this.messageService).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());

        List<BatchChangeInvisibleTimeResult> resultList = this.consumerProcessor.batchChangeInvisibleTime(
            createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000, 3000, true).get();

        assertEquals(receiptHandleMessageList.size(), resultList.size());
        for (int i = 0; i < receiptHandleMessageList.size(); i++) {
            BatchChangeInvisibleTimeResult result = resultList.get(i);
            ReceiptHandleMessage expectedHandleMessage = receiptHandleMessageList.get(i);
            assertSame(expectedHandleMessage, result.getReceiptHandleMessage());
            if (expectedHandleMessage.getReceiptHandle().isExpired()) {
                assertEquals(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, result.getProxyException().getCode());
                assertNull(result.getAckResult());
            } else {
                assertEquals(AckStatus.OK, result.getAckResult().getStatus());
                assertEquals("extra-" + expectedHandleMessage.getMessageId() + MessageConst.KEY_SEPARATOR
                    + expectedHandleMessage.getReceiptHandle().getCommitLogOffset(),
                    result.getAckResult().getExtraInfo());
                assertNull(result.getProxyException());
            }
        }
        verify(this.messageService, times(2)).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        verify(this.messageService, never()).changeInvisibleTime(any(), any(), anyString(), any(), anyLong());
    }

    @Test
    public void testBatchChangeInvisibleTimeSplitByRealTopic() throws Throwable {
        String brokerName = "brokerName1";
        String retryTopic = KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP,
            new BrokerConfig().isEnableRetryTopicV2());
        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 2; i++) {
            MessageExt normalMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName);
            receiptHandleMessageList.add(new ReceiptHandleMessage(create(normalMessage), normalMessage.getMsgId()));

            MessageExt retryMessage = createMessageExt(retryTopic, "", 0, 3000, now,
                0, 0, 0, i + 10, brokerName);
            receiptHandleMessageList.add(new ReceiptHandleMessage(create(retryMessage), retryMessage.getMsgId()));
        }

        ArgumentCaptor<List> batchHandleListCaptor = ArgumentCaptor.forClass(List.class);
        doAnswer((Answer<CompletableFuture<List<AckResult>>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            List<AckResult> ackResultList = new ArrayList<>();
            for (ReceiptHandleMessage ignored : handleMessageList) {
                AckResult ackResult = new AckResult();
                ackResult.setStatus(AckStatus.OK);
                ackResultList.add(ackResult);
            }
            return CompletableFuture.completedFuture(ackResultList);
        }).when(this.messageService).batchChangeInvisibleTime(
            any(), batchHandleListCaptor.capture(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());

        List<BatchChangeInvisibleTimeResult> resultList = this.consumerProcessor.batchChangeInvisibleTime(
            createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000, 3000, true).get();

        assertEquals(receiptHandleMessageList.size(), resultList.size());
        verify(this.messageService, times(2)).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        assertEquals(2, batchHandleListCaptor.getAllValues().size());
        assertEquals(2, batchHandleListCaptor.getAllValues().get(0).size());
        assertEquals(2, batchHandleListCaptor.getAllValues().get(1).size());
        verify(this.messageService, never()).changeInvisibleTime(any(), any(), anyString(), any(), anyLong());
    }

    @Test
    public void testBatchChangeInvisibleTimeWithSingleHandleUseSingleChange() throws Throwable {
        MessageExt messageExt = createMessageExt(TOPIC, "", 0, 3000);
        ReceiptHandle handle = create(messageExt);
        List<ReceiptHandleMessage> receiptHandleMessageList = Collections.singletonList(
            new ReceiptHandleMessage(handle, messageExt.getMsgId()));

        String newExtraInfo = "newExtraInfo";
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        innerAckResult.setPopTime(12345L);
        innerAckResult.setExtraInfo(newExtraInfo);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        List<BatchChangeInvisibleTimeResult> resultList = this.consumerProcessor.batchChangeInvisibleTime(
            createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000, 3000, true).get();

        assertEquals(1, resultList.size());
        assertSame(receiptHandleMessageList.get(0), resultList.get(0).getReceiptHandleMessage());
        assertEquals(AckStatus.OK, resultList.get(0).getAckResult().getStatus());
        assertEquals(newExtraInfo + MessageConst.KEY_SEPARATOR + handle.getCommitLogOffset(),
            resultList.get(0).getAckResult().getExtraInfo());
        assertNull(resultList.get(0).getProxyException());
        verify(this.messageService).changeInvisibleTime(any(), any(), eq(messageExt.getMsgId()), any(), anyLong());
        verify(this.messageService, never()).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
    }

    @Test
    public void testBatchChangeInvisibleTimeSplitOversizedBrokerGroup() throws Throwable {
        String brokerName = "brokerName1";
        assertEquals(1024, ConfigurationManager.getProxyConfig().getBatchChangeInvisibleTimeMaxNum());
        int batchMaxNum = ConfigurationManager.getProxyConfig().getBatchChangeInvisibleTimeMaxNum();
        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 0; i <= batchMaxNum; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName);
            receiptHandleMessageList.add(new ReceiptHandleMessage(create(brokerMessage), brokerMessage.getMsgId()));
        }

        ArgumentCaptor<List> batchHandleListCaptor = ArgumentCaptor.forClass(List.class);
        doAnswer((Answer<CompletableFuture<List<AckResult>>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            List<AckResult> ackResultList = new ArrayList<>();
            for (ReceiptHandleMessage ignored : handleMessageList) {
                AckResult ackResult = new AckResult();
                ackResult.setStatus(AckStatus.OK);
                ackResultList.add(ackResult);
            }
            return CompletableFuture.completedFuture(ackResultList);
        }).when(this.messageService).batchChangeInvisibleTime(
            any(), batchHandleListCaptor.capture(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());

        AckResult singleAckResult = new AckResult();
        singleAckResult.setStatus(AckStatus.OK);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(singleAckResult));

        List<BatchChangeInvisibleTimeResult> resultList = this.consumerProcessor.batchChangeInvisibleTime(
            createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000, 3000, true).get();

        assertEquals(receiptHandleMessageList.size(), resultList.size());
        assertEquals(batchMaxNum, batchHandleListCaptor.getValue().size());
        verify(this.messageService).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        verify(this.messageService).changeInvisibleTime(any(), any(), anyString(), any(), anyLong());
    }

    @Test
    public void testBatchChangeInvisibleTimeSplitOversizedBrokerGroupSequentially() throws Throwable {
        String brokerName = "brokerName1";
        int batchMaxNum = 2;
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(batchMaxNum);
        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 0; i < batchMaxNum * 2; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName);
            receiptHandleMessageList.add(new ReceiptHandleMessage(create(brokerMessage), brokerMessage.getMsgId()));
        }

        List<CompletableFuture<List<AckResult>>> batchFutures = new ArrayList<>();
        ArgumentCaptor<List> batchHandleListCaptor = ArgumentCaptor.forClass(List.class);
        doAnswer((Answer<CompletableFuture<List<AckResult>>>) invocation -> {
            CompletableFuture<List<AckResult>> batchFuture = new CompletableFuture<>();
            batchFutures.add(batchFuture);
            return batchFuture;
        }).when(this.messageService).batchChangeInvisibleTime(
            any(), batchHandleListCaptor.capture(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());

        CompletableFuture<List<BatchChangeInvisibleTimeResult>> resultFuture = this.consumerProcessor.batchChangeInvisibleTime(
            createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000, 3000, true);

        assertEquals(1, batchFutures.size());
        assertEquals(batchMaxNum, batchHandleListCaptor.getAllValues().get(0).size());
        assertFalse(resultFuture.isDone());

        batchFutures.get(0).complete(buildAckResultList(batchMaxNum));
        assertEquals(2, batchFutures.size());
        assertEquals(batchMaxNum, batchHandleListCaptor.getAllValues().get(1).size());
        assertFalse(resultFuture.isDone());

        batchFutures.get(1).complete(buildAckResultList(batchMaxNum));
        List<BatchChangeInvisibleTimeResult> resultList = resultFuture.get();
        assertEquals(receiptHandleMessageList.size(), resultList.size());
        verify(this.messageService, times(2)).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        verify(this.messageService, never()).changeInvisibleTime(any(), any(), anyString(), any(), anyLong());
    }

    @Test
    public void testChangeInvisibleTime() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, 1000, null, 3000, true).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(1000, requestHeaderArgumentCaptor.getValue().getInvisibleTime().longValue());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
    }

    @Test
    public void testChangeInvisibleTimeShouldPreservePopTimeWhenExtraInfoUpdated() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        long popTime = 1777203436411L;
        String newExtraInfo = "newExtraInfo";
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        innerAckResult.setPopTime(popTime);
        innerAckResult.setExtraInfo(newExtraInfo);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle,
            MessageClientIDSetter.createUniqID(), CONSUMER_GROUP, TOPIC, 1000, null, 3000, true).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(newExtraInfo + MessageConst.KEY_SEPARATOR + handle.getCommitLogOffset(), ackResult.getExtraInfo());
        assertEquals(popTime, ackResult.getPopTime());
    }

    @Test
    public void testLockBatch() throws Throwable {
        Set<MessageQueue> mqSet = new HashSet<>();
        MessageQueue mq1 = new MessageQueue(TOPIC, "broker1", 0);
        AddressableMessageQueue addressableMessageQueue1 = new AddressableMessageQueue(mq1, "127.0.0.1");
        MessageQueue mq2 = new MessageQueue(TOPIC, "broker2", 0);
        AddressableMessageQueue addressableMessageQueue2 = new AddressableMessageQueue(mq2, "127.0.0.1");
        mqSet.add(mq1);
        mqSet.add(mq2);
        when(this.topicRouteService.buildAddressableMessageQueue(any(), any())).thenAnswer(i -> new AddressableMessageQueue((MessageQueue) i.getArguments()[1], "127.0.0.1"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue1), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq1)));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue2), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq2)));
        Set<MessageQueue> result = this.consumerProcessor.lockBatchMQ(ProxyContext.create(), mqSet, CONSUMER_GROUP, CLIENT_ID, 1000)
            .get();
        assertThat(result).isEqualTo(mqSet);
    }

    @Test
    public void testLockBatchPartialSuccess() throws Throwable {
        Set<MessageQueue> mqSet = new HashSet<>();
        MessageQueue mq1 = new MessageQueue(TOPIC, "broker1", 0);
        AddressableMessageQueue addressableMessageQueue1 = new AddressableMessageQueue(mq1, "127.0.0.1");
        MessageQueue mq2 = new MessageQueue(TOPIC, "broker2", 0);
        AddressableMessageQueue addressableMessageQueue2 = new AddressableMessageQueue(mq2, "127.0.0.1");
        mqSet.add(mq1);
        mqSet.add(mq2);
        when(this.topicRouteService.buildAddressableMessageQueue(any(), any())).thenAnswer(i -> new AddressableMessageQueue((MessageQueue) i.getArguments()[1], "127.0.0.1"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue1), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq1)));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue2), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet()));
        Set<MessageQueue> result = this.consumerProcessor.lockBatchMQ(ProxyContext.create(), mqSet, CONSUMER_GROUP, CLIENT_ID, 1000)
            .get();
        assertThat(result).isEqualTo(Sets.newHashSet(mq1));
    }

    @Test
    public void testLockBatchPartialSuccessWithException() throws Throwable {
        Set<MessageQueue> mqSet = new HashSet<>();
        MessageQueue mq1 = new MessageQueue(TOPIC, "broker1", 0);
        AddressableMessageQueue addressableMessageQueue1 = new AddressableMessageQueue(mq1, "127.0.0.1");
        MessageQueue mq2 = new MessageQueue(TOPIC, "broker2", 0);
        AddressableMessageQueue addressableMessageQueue2 = new AddressableMessageQueue(mq2, "127.0.0.1");
        mqSet.add(mq1);
        mqSet.add(mq2);
        when(this.topicRouteService.buildAddressableMessageQueue(any(), any())).thenAnswer(i -> new AddressableMessageQueue((MessageQueue) i.getArguments()[1], "127.0.0.1"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue1), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq1)));
        CompletableFuture<Set<MessageQueue>> future = new CompletableFuture<>();
        future.completeExceptionally(new MQBrokerException(1, "err"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue2), any(), anyLong()))
            .thenReturn(future);
        Set<MessageQueue> result = this.consumerProcessor.lockBatchMQ(ProxyContext.create(), mqSet, CONSUMER_GROUP, CLIENT_ID, 1000)
            .get();
        assertThat(result).isEqualTo(Sets.newHashSet(mq1));
    }

    @Test
    public void testPopMessageWithToReturnFilter() throws Throwable {
        final String tag = "tag";
        final long invisibleTime = Duration.ofSeconds(15).toMillis();
        ArgumentCaptor<AddressableMessageQueue> messageQueueArgumentCaptor = ArgumentCaptor.forClass(AddressableMessageQueue.class);
        ArgumentCaptor<PopMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(PopMessageRequestHeader.class);

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, tag, 0, invisibleTime));
        PopResult innerPopResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(this.messageService.popMessage(any(), messageQueueArgumentCaptor.capture(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerPopResult));

        when(this.topicRouteService.getCurrentMessageQueueView(any(), anyString()))
            .thenReturn(mock(MessageQueueView.class));

        ArgumentCaptor<String> ackMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        when(this.messagingProcessor.ackMessage(any(), any(), ackMessageIdArgumentCaptor.capture(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        ArgumentCaptor<String> changeInvisibleTimeMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> changeInvisibleTimeInvisibleTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Boolean> changeInvisibleTimeSuspendArgumentCaptor = ArgumentCaptor.forClass(Boolean.class);
        when(this.messagingProcessor.changeInvisibleTime(any(), any(), changeInvisibleTimeMessageIdArgumentCaptor.capture(),
            anyString(), anyString(), changeInvisibleTimeInvisibleTimeArgumentCaptor.capture(), any(), anyLong(),
            changeInvisibleTimeSuspendArgumentCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        PopResult popResult = this.consumerProcessor.popMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            CONSUMER_GROUP,
            TOPIC,
            60,
            invisibleTime,
            Duration.ofSeconds(3).toMillis(),
            ConsumeInitMode.MAX,
            FilterAPI.build(TOPIC, tag, ExpressionType.TAG),
            false,
            (ctx, consumerGroup, subscriptionData, messageExt) -> {
                // Return TO_RETURN for the message
                return PopMessageResultFilter.FilterResult.TO_RETURN;
            },
            null,
            Duration.ofSeconds(3).toMillis()
        ).get();

        // Verify that changeInvisibleTime was called with suspend=true
        verify(this.messagingProcessor).changeInvisibleTime(any(), any(), eq(messageExtList.get(0).getMsgId()),
            eq(CONSUMER_GROUP), eq(TOPIC), eq(Duration.ofSeconds(1).toMillis()), eq(null),
            eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), eq(true));

        // Verify that the message was NOT added to the result list
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(0, popResult.getMsgFoundList().size());
    }

    @Test
    public void testPopMessageWithToReturnFilterUseBatchChangeInvisibleTime() throws Throwable {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        final String tag = "tag";
        final long invisibleTime = Duration.ofSeconds(15).toMillis();
        ArgumentCaptor<AddressableMessageQueue> messageQueueArgumentCaptor = ArgumentCaptor.forClass(AddressableMessageQueue.class);
        ArgumentCaptor<PopMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(PopMessageRequestHeader.class);

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, tag, 0, invisibleTime));
        messageExtList.add(createMessageExt(TOPIC, tag, 0, invisibleTime));
        PopResult innerPopResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(this.messageService.popMessage(any(), messageQueueArgumentCaptor.capture(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerPopResult));

        when(this.topicRouteService.getCurrentMessageQueueView(any(), anyString()))
            .thenReturn(mock(MessageQueueView.class));

        ArgumentCaptor<List> handleMessageListCaptor = ArgumentCaptor.forClass(List.class);
        when(this.messagingProcessor.batchChangeInvisibleTime(any(), handleMessageListCaptor.capture(),
            anyString(), anyString(), anyLong(), anyLong(), anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        PopResult popResult = this.consumerProcessor.popMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            CONSUMER_GROUP,
            TOPIC,
            60,
            invisibleTime,
            Duration.ofSeconds(3).toMillis(),
            ConsumeInitMode.MAX,
            FilterAPI.build(TOPIC, tag, ExpressionType.TAG),
            false,
            (ctx, consumerGroup, subscriptionData, messageExt) -> PopMessageResultFilter.FilterResult.TO_RETURN,
            null,
            Duration.ofSeconds(3).toMillis()
        ).get();

        verify(this.messagingProcessor).batchChangeInvisibleTime(any(), anyList(),
            eq(CONSUMER_GROUP), eq(TOPIC), eq(Duration.ofSeconds(1).toMillis()),
            eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), eq(true));
        verify(this.messagingProcessor, never()).changeInvisibleTime(any(), any(), anyString(),
            anyString(), anyString(), anyLong(), any(), anyLong(), anyBoolean());
        assertEquals(2, handleMessageListCaptor.getValue().size());
        assertEquals(messageExtList.get(0).getMsgId(),
            ((ReceiptHandleMessage) handleMessageListCaptor.getValue().get(0)).getMessageId());
        assertEquals(messageExtList.get(1).getMsgId(),
            ((ReceiptHandleMessage) handleMessageListCaptor.getValue().get(1)).getMessageId());
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(0, popResult.getMsgFoundList().size());
    }

    @Test
    public void testPopMessageWithSingleToReturnFilterUseSingleChangeInvisibleTime() throws Throwable {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        final String tag = "tag";
        final long invisibleTime = Duration.ofSeconds(15).toMillis();
        ArgumentCaptor<AddressableMessageQueue> messageQueueArgumentCaptor = ArgumentCaptor.forClass(AddressableMessageQueue.class);
        ArgumentCaptor<PopMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(PopMessageRequestHeader.class);

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, tag, 0, invisibleTime));
        PopResult innerPopResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(this.messageService.popMessage(any(), messageQueueArgumentCaptor.capture(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerPopResult));

        when(this.topicRouteService.getCurrentMessageQueueView(any(), anyString()))
            .thenReturn(mock(MessageQueueView.class));

        when(this.messagingProcessor.changeInvisibleTime(any(), any(), anyString(),
            anyString(), anyString(), anyLong(), any(), anyLong(), anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        PopResult popResult = this.consumerProcessor.popMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            CONSUMER_GROUP,
            TOPIC,
            60,
            invisibleTime,
            Duration.ofSeconds(3).toMillis(),
            ConsumeInitMode.MAX,
            FilterAPI.build(TOPIC, tag, ExpressionType.TAG),
            false,
            (ctx, consumerGroup, subscriptionData, messageExt) -> PopMessageResultFilter.FilterResult.TO_RETURN,
            null,
            Duration.ofSeconds(3).toMillis()
        ).get();

        verify(this.messagingProcessor).changeInvisibleTime(any(), any(), eq(messageExtList.get(0).getMsgId()),
            eq(CONSUMER_GROUP), eq(TOPIC), eq(Duration.ofSeconds(1).toMillis()), eq(null),
            eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), eq(true));
        verify(this.messagingProcessor, never()).batchChangeInvisibleTime(any(), anyList(),
            anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(0, popResult.getMsgFoundList().size());
    }

    @Test
    public void testChangeInvisibleTimeWithSuspendFalse() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, 1000, null, 3000, false).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(1000, requestHeaderArgumentCaptor.getValue().getInvisibleTime().longValue());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
        assertFalse("Suspend should be false", requestHeaderArgumentCaptor.getValue().isSuspend());
    }

    @Test
    public void testChangeInvisibleTimeWithSuspendTrue() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, 1000, null, 3000, true).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(1000, requestHeaderArgumentCaptor.getValue().getInvisibleTime().longValue());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
        assertTrue("Suspend should be true", requestHeaderArgumentCaptor.getValue().isSuspend());
    }

    private List<AckResult> buildAckResultList(int size) {
        List<AckResult> ackResultList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            AckResult ackResult = new AckResult();
            ackResult.setStatus(AckStatus.OK);
            ackResultList.add(ackResult);
        }
        return ackResultList;
    }
}
