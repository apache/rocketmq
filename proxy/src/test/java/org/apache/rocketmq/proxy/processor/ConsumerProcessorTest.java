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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerProcessorTest extends BaseProcessorTest {

    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String TOPIC = "topic";
    private static final String CLIENT_ID = "clientId";

    private ConsumerProcessor consumerProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        ReceiptHandleProcessor receiptHandleProcessor = new ReceiptHandleProcessor(messagingProcessor);
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
        when(this.messagingProcessor.ackMessage(any(), any(), ackMessageIdArgumentCaptor.capture(), anyString(), anyString(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        ArgumentCaptor<String> toDLQMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        when(this.messagingProcessor.forwardMessageToDeadLetterQueue(any(), any(), toDLQMessageIdArgumentCaptor.capture(), anyString(), anyString(), anyLong()))
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
            CONSUMER_GROUP, TOPIC, 3000).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
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
            CONSUMER_GROUP, TOPIC, 1000, 3000).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(1000, requestHeaderArgumentCaptor.getValue().getInvisibleTime().longValue());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
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
}
