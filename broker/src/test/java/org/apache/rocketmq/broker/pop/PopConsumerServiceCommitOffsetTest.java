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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

public class PopConsumerServiceCommitOffsetTest {

    private static final long INVISIBLE_TIME = TimeUnit.SECONDS.toMillis(20);

    private final String clientHost = "127.0.0.1:8888";
    private final String groupId = "groupId";
    private final String topicId = "topicId";
    private final int queueId = 2;
    private final String attemptId = UUID.randomUUID().toString().toUpperCase();
    private final String filePath = PopConsumerRocksdbStoreTest.getRandomStorePath();

    private BrokerController brokerController;
    private ConsumerOffsetManager consumerOffsetManager;
    private PopConsumerService consumerService;

    private void init(boolean enablePopBufferMerge) {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnablePopBufferMerge(enablePopBufferMerge);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(filePath);

        consumerOffsetManager = Mockito.mock(ConsumerOffsetManager.class);
        PopMessageProcessor popMessageProcessor = Mockito.mock(PopMessageProcessor.class);

        brokerController = Mockito.mock(BrokerController.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        Mockito.when(brokerController.getTopicConfigManager()).thenReturn(Mockito.mock(TopicConfigManager.class));
        Mockito.when(brokerController.getSubscriptionGroupManager())
            .thenReturn(Mockito.mock(SubscriptionGroupManager.class));
        Mockito.when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        Mockito.when(brokerController.getConsumerOrderInfoManager())
            .thenReturn(Mockito.mock(ConsumerOrderInfoManager.class));

        consumerService = new PopConsumerService(brokerController);
    }

    @After
    public void shutdown() throws IOException {
        FileUtils.deleteDirectory(new File(filePath));
    }

    private PopConsumerContext popContext(boolean fifo) {
        return new PopConsumerContext(clientHost, System.currentTimeMillis(),
            INVISIBLE_TIME, groupId, fifo, ConsumeInitMode.MIN, attemptId);
    }

    private GetMessageResult foundResult(long nextBeginOffset) {
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.FOUND);
        result.setNextBeginOffset(nextBeginOffset);
        result.getMessageQueueOffset().add(nextBeginOffset - 1);
        return result;
    }

    private PopConsumerCache getConsumerCache() throws IllegalAccessException {
        return (PopConsumerCache) FieldUtils.readField(consumerService, "popConsumerCache", true);
    }

    private void verifyCommitted(long offset) {
        Mockito.verify(consumerOffsetManager).commitOffset(
            anyString(), eq(groupId), eq(topicId), eq(queueId), eq(offset));
    }

    private void verifyNeverCommitted() {
        Mockito.verify(consumerOffsetManager, Mockito.never()).commitOffset(
            anyString(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test
    public void commitToNextBeginOffsetWithoutCache() {
        init(false);
        consumerService.getPopConsumerStore().start();

        PopConsumerContext context = popContext(false);
        consumerService.handleGetMessageResult(context, foundResult(110L),
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100L);

        // staged only, not committed before the records are written
        verifyNeverCommitted();
        Assert.assertEquals(1, context.getPendingCommitList().size());
        Assert.assertEquals(110L, context.getPendingCommitList().get(0).getCommitOffset());

        consumerService.getPopConsumerStore().writeRecords(context.getPopConsumerRecordList());
        consumerService.commitPendingOffset(context);
        verifyCommitted(110L);

        consumerService.shutdown();
    }

    @Test
    public void commitPullOffsetAlways() {
        init(false);
        PopConsumerContext context = popContext(false);
        consumerService.handleGetMessageResult(context, foundResult(110L),
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100L);
        Mockito.verify(consumerOffsetManager).commitPullOffset(
            anyString(), eq(groupId), eq(topicId), eq(queueId), eq(110L));
    }

    @Test
    public void commitWhenNoMatchedMessage() {
        init(false);
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
        result.setNextBeginOffset(110L);

        PopConsumerContext context = popContext(false);
        consumerService.handleGetMessageResult(context, result,
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100L);
        Assert.assertFalse(context.isFound());

        consumerService.commitPendingOffset(context);
        verifyCommitted(110L);
    }

    @Test
    public void commitBoundedByMinOffsetInCache() throws IllegalAccessException {
        init(true);
        PopConsumerCache consumerCache = getConsumerCache();
        Assert.assertNotNull(consumerCache);

        PopConsumerContext context = popContext(false);
        consumerService.handleGetMessageResult(context, foundResult(110L),
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100L);
        consumerCache.writeRecords(Collections.singletonList(new PopConsumerRecord(
            System.currentTimeMillis(), groupId, topicId, queueId, 0, INVISIBLE_TIME, 105L, attemptId)));

        consumerService.commitPendingOffset(context);

        // bounded by the in-flight record at 105 rather than jumping to 110
        verifyCommitted(105L);
        // the remaining part converges in PopConsumerCache#cleanupRecords once the record is acked
        Assert.assertEquals(105L, consumerCache.getMinOffsetInCache(groupId, topicId, queueId));
    }

    @Test
    public void commitBoundedWhenCacheFull() throws IllegalAccessException {
        init(true);
        PopConsumerCache consumerCache = getConsumerCache();
        consumerCache.writeRecords(Collections.singletonList(new PopConsumerRecord(
            System.currentTimeMillis(), groupId, topicId, queueId, 0, INVISIBLE_TIME, 100L, attemptId)));

        // the later batch bypassed the cache, but the cached record at 100 is still in flight
        PopConsumerContext context = popContext(false);
        consumerService.handleGetMessageResult(context, foundResult(120L),
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 110L);
        consumerService.commitPendingOffset(context);

        verifyCommitted(100L);
    }

    @Test
    public void skipCommitWhenOffsetReset() {
        init(false);
        Mockito.when(consumerOffsetManager.hasOffsetReset(topicId, groupId, queueId)).thenReturn(true);

        PopConsumerContext context = popContext(false);
        consumerService.handleGetMessageResult(context, foundResult(110L),
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100L);
        consumerService.commitPendingOffset(context);

        verifyNeverCommitted();
    }

    @Test
    public void commitIsIdempotent() {
        init(false);
        PopConsumerContext context = popContext(false);
        consumerService.handleGetMessageResult(context, foundResult(110L),
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100L);

        consumerService.commitPendingOffset(context);
        // the offset store caught up after the first commit
        Mockito.when(consumerOffsetManager.queryOffset(groupId, topicId, queueId)).thenReturn(110L);
        consumerService.commitPendingOffset(context);

        Mockito.verify(consumerOffsetManager, Mockito.times(1)).commitOffset(
            anyString(), eq(groupId), eq(topicId), eq(queueId), eq(110L));
    }

    @Test
    public void fifoKeepsInlineCommit() {
        init(false);
        PopConsumerContext context = popContext(true);
        consumerService.handleGetMessageResult(context, foundResult(110L),
            topicId, queueId, PopConsumerRecord.RetryType.NORMAL_TOPIC, 100L);

        Assert.assertNull(context.getPendingCommitList());
        // fifo commits the batch start offset inline when messages are found
        verifyCommitted(100L);
        Mockito.verify(consumerOffsetManager, Mockito.never()).commitPullOffset(
            anyString(), anyString(), anyString(), anyInt(), anyLong());
    }
}
