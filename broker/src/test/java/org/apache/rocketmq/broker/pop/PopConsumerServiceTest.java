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
package org.apache.rocketmq.broker.pop;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class PopConsumerServiceTest {

    private final String clientHost = "127.0.0.1:8888";
    private final String groupId = "groupId";
    private final String topicId = "topicId";
    private final int queueId = 2;
    private final String attemptId = UUID.randomUUID().toString().toUpperCase();
    private final String filePath = PopConsumerRocksdbStoreTest.getRandomStorePath();

    private BrokerController brokerController;
    private PopConsumerService consumerService;

    @Before
    public void init() throws IOException {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnablePopLog(true);
        brokerConfig.setEnablePopBufferMerge(true);
        brokerConfig.setEnablePopMessageThreshold(true);
        brokerConfig.setPopInflightMessageThreshold(100);
        brokerConfig.setPopConsumerKVServiceLog(true);
        brokerConfig.setEnableRetryTopicV2(true);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(filePath);

        TopicConfigManager topicConfigManager = Mockito.mock(TopicConfigManager.class);
        ConsumerOffsetManager consumerOffsetManager = Mockito.mock(ConsumerOffsetManager.class);
        PopMessageProcessor popMessageProcessor = Mockito.mock(PopMessageProcessor.class);
        PopLongPollingService popLongPollingService = Mockito.mock(PopLongPollingService.class);
        ConsumerOrderInfoManager consumerOrderInfoManager = Mockito.mock(ConsumerOrderInfoManager.class);

        brokerController = Mockito.mock(BrokerController.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        Mockito.when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        Mockito.when(popMessageProcessor.getPopLongPollingService()).thenReturn(popLongPollingService);
        Mockito.when(brokerController.getConsumerOrderInfoManager()).thenReturn(consumerOrderInfoManager);

        consumerService = new PopConsumerService(brokerController);
    }

    @After
    public void shutdown() throws IOException {
        FileUtils.deleteDirectory(new File(filePath));
    }

    public PopConsumerRecord getConsumerTestRecord() {
        PopConsumerRecord popConsumerRecord = new PopConsumerRecord();
        popConsumerRecord.setPopTime(System.currentTimeMillis());
        popConsumerRecord.setGroupId(groupId);
        popConsumerRecord.setTopicId(topicId);
        popConsumerRecord.setQueueId(queueId);
        popConsumerRecord.setRetryFlag(PopConsumerRecord.RetryType.NORMAL_TOPIC.getCode());
        popConsumerRecord.setAttemptTimes(0);
        popConsumerRecord.setInvisibleTime(TimeUnit.SECONDS.toMillis(20));
        popConsumerRecord.setAttemptId(UUID.randomUUID().toString().toUpperCase());
        return popConsumerRecord;
    }

    @Test
    public void isPopShouldStopTest() throws IllegalAccessException {
        Assert.assertFalse(consumerService.isPopShouldStop(groupId, topicId, queueId));
        PopConsumerCache consumerCache = (PopConsumerCache) FieldUtils.readField(
            consumerService, "popConsumerCache", true);
        for (int i = 0; i < 100; i++) {
            PopConsumerRecord record = getConsumerTestRecord();
            record.setOffset(i);
            consumerCache.writeRecords(Collections.singletonList(record));
        }
        Assert.assertTrue(consumerService.isPopShouldStop(groupId, topicId, queueId));
    }

    @Test
    public void pendingFilterCountTest() throws ConsumeQueueException {
        MessageStore messageStore = Mockito.mock(MessageStore.class);
        Mockito.when(messageStore.getMaxOffsetInQueue(topicId, queueId)).thenReturn(100L);
        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        ConsumerOffsetManager consumerOffsetManager = brokerController.getConsumerOffsetManager();
        Mockito.when(consumerOffsetManager.queryOffset(groupId, topicId, queueId)).thenReturn(20L);
        Assert.assertEquals(consumerService.getPendingFilterCount(groupId, topicId, queueId), 80L);
    }

    @Test
    public void pendingFilterCountTest_consumeQueueException() throws ConsumeQueueException {
        MessageStore messageStore = Mockito.mock(MessageStore.class);
        Mockito.when(messageStore.getMaxOffsetInQueue(topicId, queueId)).thenThrow(new ConsumeQueueException("queue fail!"));
        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        Assert.assertThrows(RuntimeException.class, () -> consumerService.getPendingFilterCount(groupId, topicId, queueId));
        verify(brokerController, never()).getConsumerOffsetManager();
    }

    private MessageExt getMessageExt() {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topicId);
        messageExt.setQueueId(queueId);
        messageExt.setBody(new byte[128]);
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 8080));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 8080));
        messageExt.putUserProperty("Key", "Value");
        return messageExt;
    }

    @Test
    public void recodeRetryMessageTest() throws Exception {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.FOUND);

        // result is empty
        SelectMappedBufferResult bufferResult = new SelectMappedBufferResult(
            0, ByteBuffer.allocate(10), 10, null);
        getMessageResult.addMessage(bufferResult);
        getMessageResult.getMessageMapedList().clear();
        GetMessageResult result = consumerService.recodeRetryMessage(
            getMessageResult, topicId, 0, 100, 200);
        Assert.assertEquals(0, result.getMessageMapedList().size());

        ByteBuffer buffer = ByteBuffer.wrap(
            MessageDecoder.encode(getMessageExt(), false));
        getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.FOUND);
        getMessageResult.addMessage(new SelectMappedBufferResult(
            0, buffer, buffer.remaining(), null));
        result = consumerService.recodeRetryMessage(
            getMessageResult, topicId, 0, 100, 200);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getMessageMapedList().size());
    }

    @Test
    public void addGetMessageResultTest() {
        PopConsumerContext context = new PopConsumerContext(
            clientHost, System.currentTimeMillis(), 20000, groupId, false, attemptId);
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        result.getMessageQueueOffset().add(100L);
        consumerService.addGetMessageResult(
            context, result, topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100);
        Assert.assertEquals(1, context.getGetMessageResultList().size());
    }

    @Test
    public void getMessageAsyncTest() throws Exception {
        MessageStore messageStore = Mockito.mock(MessageStore.class);
        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        Mockito.when(messageStore.getMessageAsync(groupId, topicId, queueId, 0, 10, null))
            .thenReturn(CompletableFuture.completedFuture(null));
        GetMessageResult getMessageResult = consumerService.getMessageAsync(
            "127.0.0.1:8888", groupId, topicId, queueId, 0, 10, null).join();
        Assert.assertNull(getMessageResult);

        // success when first get message
        GetMessageResult firstGetMessageResult = new GetMessageResult();
        firstGetMessageResult.setStatus(GetMessageStatus.FOUND);
        Mockito.when(messageStore.getMessageAsync(groupId, topicId, queueId, 0, 10, null))
            .thenReturn(CompletableFuture.completedFuture(firstGetMessageResult));
        getMessageResult = consumerService.getMessageAsync(
            "127.0.0.1:8888", groupId, topicId, queueId, 0, 10, null).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        // reset offset from server
        firstGetMessageResult.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
        firstGetMessageResult.setNextBeginOffset(25);
        GetMessageResult resetGetMessageResult = new GetMessageResult();
        resetGetMessageResult.setStatus(GetMessageStatus.FOUND);
        Mockito.when(messageStore.getMessageAsync(groupId, topicId, queueId, 25, 10, null))
            .thenReturn(CompletableFuture.completedFuture(resetGetMessageResult));
        getMessageResult = consumerService.getMessageAsync(
            "127.0.0.1:8888", groupId, topicId, queueId, 0, 10, null).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        // fifo block
        PopConsumerContext context = new PopConsumerContext(
            clientHost, System.currentTimeMillis(), 20000, groupId, false, attemptId);
        consumerService.setFifoBlocked(context, groupId, topicId, queueId, Collections.singletonList(100L));
        Mockito.when(brokerController.getConsumerOrderInfoManager()
            .checkBlock(anyString(), anyString(), anyString(), anyInt(), anyLong())).thenReturn(true);
        Assert.assertTrue(consumerService.isFifoBlocked(context, groupId, topicId, queueId));

        // get message async normal
        CompletableFuture<PopConsumerContext> future = CompletableFuture.completedFuture(context);
        Assert.assertEquals(0L, consumerService.getMessageAsync(future, clientHost, groupId, topicId, queueId,
            10, null, PopConsumerRecord.RetryType.NORMAL_TOPIC).join().getRestCount());

        // get message result full, no need get again
        for (int i = 0; i < 10; i++) {
            ByteBuffer buffer = ByteBuffer.wrap(MessageDecoder.encode(getMessageExt(), false));
            getMessageResult.addMessage(new SelectMappedBufferResult(
                0, buffer, buffer.remaining(), null), i);
        }
        context.addGetMessageResult(getMessageResult, topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 0);

        Mockito.when(brokerController.getMessageStore().getMaxOffsetInQueue(topicId, queueId)).thenReturn(100L);
        Mockito.when(brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, queueId)).thenReturn(0L);
        Assert.assertEquals(100L, consumerService.getMessageAsync(future, clientHost, groupId, topicId, queueId,
            10, null, PopConsumerRecord.RetryType.NORMAL_TOPIC).join().getRestCount());

        // fifo block test
        context = new PopConsumerContext(
            clientHost, System.currentTimeMillis(), 20000, groupId, true, attemptId);
        future = CompletableFuture.completedFuture(context);
        Assert.assertEquals(0L, consumerService.getMessageAsync(future, clientHost, groupId, topicId, queueId,
            10, null, PopConsumerRecord.RetryType.NORMAL_TOPIC).join().getRestCount());
    }

    @Test
    public void popAsyncTest() {
        PopConsumerService consumerServiceSpy = Mockito.spy(consumerService);
        TopicConfigManager topicConfigManager = Mockito.mock(TopicConfigManager.class);
        Mockito.when(topicConfigManager.selectTopicConfig(topicId)).thenReturn(new TopicConfig(
            topicId, 2, 2, PermName.PERM_READ | PermName.PERM_WRITE, 0));
        Mockito.when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);

        String[] retryTopic = new String[] {
            KeyBuilder.buildPopRetryTopicV1(topicId, groupId),
            KeyBuilder.buildPopRetryTopicV2(topicId, groupId)
        };

        for (String retry : retryTopic) {
            GetMessageResult getMessageResult = new GetMessageResult();
            getMessageResult.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            getMessageResult.setMinOffset(0L);
            getMessageResult.setMaxOffset(1L);
            getMessageResult.setNextBeginOffset(1L);
            Mockito.doReturn(CompletableFuture.completedFuture(getMessageResult))
                .when(consumerServiceSpy).getMessageAsync(clientHost, groupId, retry, 0, 0, 10, null);
            Mockito.doReturn(CompletableFuture.completedFuture(getMessageResult))
                .when(consumerServiceSpy).getMessageAsync(clientHost, groupId, retry, 0, 0, 8, null);
        }

        for (int i = -1; i < 2; i++) {
            GetMessageResult getMessageResult = new GetMessageResult();
            getMessageResult.setStatus(GetMessageStatus.FOUND);
            getMessageResult.setMinOffset(0L);
            getMessageResult.setMaxOffset(1L);
            getMessageResult.setNextBeginOffset(1L);
            getMessageResult.addMessage(Mockito.mock(SelectMappedBufferResult.class), 1L);

            Mockito.doReturn(CompletableFuture.completedFuture(getMessageResult))
                .when(consumerServiceSpy).getMessageAsync(clientHost, groupId, topicId, i, 0, 8, null);
            Mockito.doReturn(CompletableFuture.completedFuture(getMessageResult))
                .when(consumerServiceSpy).getMessageAsync(clientHost, groupId, topicId, i, 0, 9, null);
            Mockito.doReturn(CompletableFuture.completedFuture(getMessageResult))
                .when(consumerServiceSpy).getMessageAsync(clientHost, groupId, topicId, i, 0, 10, null);
        }

        // pop broker
        consumerServiceSpy.popAsync(clientHost, System.currentTimeMillis(),
            20000, groupId, topicId, -1, 10, false, attemptId, null).join();
    }

    @Test
    public void ackAsyncTest() {
        long current = System.currentTimeMillis();
        consumerService.getPopConsumerStore().start();
        consumerService.ackAsync(
            current, 10, groupId, topicId, queueId, 100).join();
        consumerService.changeInvisibilityDuration(current, 10,
            current + 100, 10, groupId, topicId, queueId, 100);
        consumerService.shutdown();
    }

    @Test
    public void reviveRetryTest() {
        Mockito.when(brokerController.getTopicConfigManager().selectTopicConfig(topicId)).thenReturn(null);
        Mockito.when(brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, 0)).thenReturn(-1L);

        consumerService.createRetryTopicIfNeeded(groupId, topicId);
        consumerService.clearCache(groupId, topicId, queueId);
        MessageExt messageExt = new MessageExt();
        messageExt.setBody("body".getBytes());
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setFlag(0);
        messageExt.setSysFlag(0);
        messageExt.setReconsumeTimes(1);
        messageExt.putUserProperty("key", "value");

        PopConsumerRecord record = new PopConsumerRecord();
        record.setTopicId("topic");
        record.setGroupId("group");
        Mockito.when(brokerController.getBrokerStatsManager()).thenReturn(Mockito.mock(BrokerStatsManager.class));
        Mockito.when(brokerController.getEscapeBridge()).thenReturn(Mockito.mock(EscapeBridge.class));
        Mockito.when(brokerController.getEscapeBridge().putMessageToSpecificQueue(any(MessageExtBrokerInner.class)))
            .thenReturn(new PutMessageResult(
                PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));

        PopConsumerService consumerServiceSpy = Mockito.spy(consumerService);
        Mockito.doNothing().when(consumerServiceSpy).createRetryTopicIfNeeded(any(), any());
        Assert.assertTrue(consumerServiceSpy.reviveRetry(record, messageExt));

        // write message error
        Mockito.when(brokerController.getEscapeBridge().putMessageToSpecificQueue(any(MessageExtBrokerInner.class)))
            .thenReturn(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR,
                new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        Assert.assertFalse(consumerServiceSpy.reviveRetry(record, messageExt));

        // revive backoff
        consumerService.getPopConsumerStore().start();
        List<PopConsumerRecord> consumerRecordList = IntStream.range(0, 3)
            .mapToObj(i -> {
                PopConsumerRecord temp = new PopConsumerRecord();
                temp.setPopTime(0);
                temp.setInvisibleTime(20 * 1000);
                temp.setTopicId("topic");
                temp.setGroupId("group");
                temp.setQueueId(2);
                temp.setOffset(i);
                return temp;
            })
            .collect(Collectors.toList());
        consumerService.getPopConsumerStore().writeRecords(consumerRecordList);

        Mockito.doReturn(CompletableFuture.completedFuture(null))
            .when(consumerServiceSpy).getMessageAsync(any(PopConsumerRecord.class));
        consumerServiceSpy.revive(new AtomicLong(20 * 1000), 1);

        Mockito.doReturn(CompletableFuture.completedFuture(
                Triple.of(null, "GetMessageResult is null", false)))
            .when(consumerServiceSpy).getMessageAsync(any(PopConsumerRecord.class));
        consumerServiceSpy.revive(new AtomicLong(20 * 1000), 1);

        Mockito.doReturn(CompletableFuture.completedFuture(
                Triple.of(Mockito.mock(MessageExt.class), null, false)))
            .when(consumerServiceSpy).getMessageAsync(any(PopConsumerRecord.class));
        consumerServiceSpy.revive(new AtomicLong(20 * 1000), 1);
        consumerService.shutdown();
    }

    @Test
    public void reviveBackoffRetryTest() {
        Mockito.when(brokerController.getEscapeBridge()).thenReturn(Mockito.mock(EscapeBridge.class));
        PopConsumerService consumerServiceSpy = Mockito.spy(consumerService);

        consumerService.getPopConsumerStore().start();

        long popTime = 1000000000L;
        long invisibleTime = 60 * 1000L;
        PopConsumerRecord record = new PopConsumerRecord();
        record.setPopTime(popTime);
        record.setInvisibleTime(invisibleTime);
        record.setTopicId("topic");
        record.setGroupId("group");
        record.setQueueId(0);
        record.setOffset(0);
        consumerService.getPopConsumerStore().writeRecords(Collections.singletonList(record));

        Mockito.doReturn(CompletableFuture.completedFuture(Triple.of(Mockito.mock(MessageExt.class), "", false)))
            .when(consumerServiceSpy).getMessageAsync(any(PopConsumerRecord.class));
        Mockito.when(brokerController.getEscapeBridge().putMessageToSpecificQueue(any(MessageExtBrokerInner.class))).thenReturn(
            new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))
        );

        long visibleTimestamp = popTime + invisibleTime;

        // revive fails
        Assert.assertEquals(1, consumerServiceSpy.revive(new AtomicLong(visibleTimestamp), 1));
        // should be invisible now
        Assert.assertEquals(0, consumerService.getPopConsumerStore().scanExpiredRecords(0, visibleTimestamp, 1).size());
        // will be visible again in 10 seconds
        Assert.assertEquals(1, consumerService.getPopConsumerStore().scanExpiredRecords(visibleTimestamp, System.currentTimeMillis() + visibleTimestamp + 10 * 1000, 1).size());

        consumerService.shutdown();
    }

    @Test
    public void transferToFsStoreTest() {
        Assert.assertNotNull(consumerService.getServiceName());
        List<PopConsumerRecord> consumerRecordList = IntStream.range(0, 3)
            .mapToObj(i -> {
                PopConsumerRecord temp = new PopConsumerRecord();
                temp.setPopTime(0);
                temp.setInvisibleTime(20 * 1000);
                temp.setTopicId("topic");
                temp.setGroupId("group");
                temp.setQueueId(2);
                temp.setOffset(i);
                return temp;
            })
            .collect(Collectors.toList());

        Mockito.when(brokerController.getPopMessageProcessor().buildCkMsg(any(), anyInt()))
            .thenReturn(new MessageExtBrokerInner());
        Mockito.when(brokerController.getMessageStore()).thenReturn(Mockito.mock(MessageStore.class));
        Mockito.when(brokerController.getMessageStore().asyncPutMessage(any()))
            .thenReturn(CompletableFuture.completedFuture(
                new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))));

        consumerService.start();
        consumerService.getPopConsumerStore().writeRecords(consumerRecordList);
        consumerService.transferToFsStore();
        consumerService.shutdown();
    }
}