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

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.local.LocalChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.proxy.common.BatchRenewEvent;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroupKey;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.receipt.DefaultReceiptHandleManager;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

public class ReceiptHandleBatchCompletionTest extends BaseProcessorTest {
    private final List<List<ReceiptHandleMessage>> requests = new ArrayList<>();
    private final List<CompletableFuture<List<AckResult>>> responses = new ArrayList<>();
    private final List<BatchRenewEvent> events = new ArrayList<>();
    private TestReceiptHandleManager manager;
    private ExecutorService executor;
    private ReceiptHandleProcessor receiptProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        executor = MoreExecutors.newDirectExecutorService();
        ConsumerProcessor consumerProcessor = new ConsumerProcessor(messagingProcessor, serviceManager, executor);
        receiptProcessor = new ReceiptHandleProcessor(messagingProcessor, serviceManager);
        manager = new TestReceiptHandleManager();
        doAnswer(invocation -> consumerProcessor.changeInvisibleTime(
            invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2),
            invocation.getArgument(3), invocation.getArgument(4), invocation.getArgument(5), invocation.getArgument(6),
            MessagingProcessor.DEFAULT_TIMEOUT_MILLS, false))
            .when(messagingProcessor).changeInvisibleTime(any(), any(), anyString(), anyString(), anyString(), anyLong(), any());
        doAnswer(invocation -> consumerProcessor.batchChangeInvisibleTime(
            invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2),
            invocation.getArgument(3), invocation.getArgument(4), invocation.getArgument(5), invocation.getArgument(6)))
            .when(messagingProcessor).batchChangeInvisibleTime(
                any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        doAnswer(invocation -> {
            requests.add(new ArrayList<>(invocation.getArgument(1)));
            CompletableFuture<List<AckResult>> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        }).when(messageService).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
    }

    @After
    public void after() {
        try {
            manager.shutdown();
            receiptProcessor.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            executor.shutdownNow();
            super.after();
        }
    }

    @Test
    public void testFastBrokerReleasesHandlesBeforeSlowBrokerCompletes() {
        MessageReceiptHandle fast = message("fast", ReceiptHandle.NORMAL_TOPIC, 0);
        MessageReceiptHandle slow = message("slow", ReceiptHandle.NORMAL_TOPIC, 1);
        manager.renew(fast, slow, message("fast", ReceiptHandle.NORMAL_TOPIC, 2),
            message("slow", ReceiptHandle.NORMAL_TOPIC, 3));
        assertEquals(2, requests.size());
        completeSuccess(requestIndex("fast", ReceiptHandle.NORMAL_TOPIC));
        assertEquals(1, fast.getRenewTimes());
        assertEquals(0, slow.getRenewTimes());
        assertTrue(events.get(0).getFuture().isDone());
        assertFalse(events.get(1).getFuture().isDone());
        MessageReceiptHandle removed = manager.group.remove(fast.getMessageId(), fast.getOriginalReceiptHandleStr());
        assertEquals(fast.getReceiptHandleStr(), removed.getReceiptHandleStr());
        assertFalse(fast.getOriginalReceiptHandleStr().equals(removed.getReceiptHandleStr()));
        completeSuccess(requestIndex("slow", ReceiptHandle.NORMAL_TOPIC));
        assertEquals(1, slow.getRenewTimes());
    }

    @Test
    public void testCompletedChunkReleasesHandlesBeforeNextChunkCompletes() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        MessageReceiptHandle first = message("broker", ReceiptHandle.NORMAL_TOPIC, 0);
        MessageReceiptHandle last = message("broker", ReceiptHandle.NORMAL_TOPIC, 3);
        manager.renew(first, message("broker", ReceiptHandle.NORMAL_TOPIC, 1),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 2), last);
        assertEquals(1, requests.size());
        completeSuccess(0);
        assertEquals(2, requests.size());
        assertEquals(1, first.getRenewTimes());
        assertEquals(0, last.getRenewTimes());
        assertTrue(events.get(0).getFuture().isDone());
        assertFalse(events.get(1).getFuture().isDone());
        assertEquals(first, manager.group.remove(first.getMessageId(), first.getOriginalReceiptHandleStr()));
        completeSuccess(1);
        assertEquals(1, last.getRenewTimes());
    }

    @Test
    public void testQueuedChunkDoesNotLockHandlesBeforeSubmission() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        MessageReceiptHandle last = message("broker", ReceiptHandle.NORMAL_TOPIC, 2);
        manager.renew(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 1), last);
        assertEquals(last, manager.group.remove(last.getMessageId(), last.getOriginalReceiptHandleStr()));
        completeSuccess(0);
        assertEquals(1, requests.size());
        assertEquals(1, events.size());
    }

    @Test
    public void testQueuedChunkSkipsHandlesAlreadyRenewed() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        MessageReceiptHandle last = message("broker", ReceiptHandle.NORMAL_TOPIC, 2);
        manager.renew(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 1), last);
        String renewed = ReceiptHandle.builder().startOffset(0).retrieveTime(System.currentTimeMillis())
            .invisibleTime(60000).reviveQueueId(1).topicType(ReceiptHandle.NORMAL_TOPIC).brokerName("broker")
            .queueId(0).offset(2).commitLogOffset(102).build().encode();
        manager.group.computeIfPresent(last.getMessageId(), last.getOriginalReceiptHandleStr(), current -> {
            current.updateReceiptHandle(renewed);
            return CompletableFuture.completedFuture(current);
        });
        completeSuccess(0);
        assertEquals(1, requests.size());
        assertEquals(1, events.size());
        assertEquals(renewed, manager.group.remove(last.getMessageId(), last.getOriginalReceiptHandleStr()).getReceiptHandleStr());
    }

    @Test
    public void testFinalSingleHandleChunkCompletesSeparately() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        CompletableFuture<AckResult> lastResponse = new CompletableFuture<>();
        doAnswer(invocation -> lastResponse).when(messageService).changeInvisibleTime(
            any(), any(), anyString(), any(), anyLong());
        MessageReceiptHandle last = message("broker", ReceiptHandle.NORMAL_TOPIC, 2);
        manager.renew(message("broker", ReceiptHandle.NORMAL_TOPIC, 0),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 1), last);
        completeSuccess(0);
        assertEquals(2, events.size());
        assertTrue(events.get(0).getFuture().isDone());
        assertFalse(events.get(1).getFuture().isDone());
        AckResult result = new AckResult();
        result.setStatus(AckStatus.OK);
        result.setExtraInfo(last.getReceiptHandleStr().substring(0, last.getReceiptHandleStr().lastIndexOf(MessageConst.KEY_SEPARATOR)));
        lastResponse.complete(result);
        assertEquals(1, last.getRenewTimes());
    }

    @Test
    public void testRetryTopicCompletesIndependentlyOnSameBroker() {
        MessageReceiptHandle normal = message("broker", ReceiptHandle.NORMAL_TOPIC, 0);
        MessageReceiptHandle retry = message("broker", ReceiptHandle.RETRY_TOPIC_V2, 1);
        manager.renew(normal, retry, message("broker", ReceiptHandle.NORMAL_TOPIC, 2),
            message("broker", ReceiptHandle.RETRY_TOPIC_V2, 3));
        assertEquals(2, requests.size());
        completeSuccess(requestIndex("broker", ReceiptHandle.RETRY_TOPIC_V2));
        assertEquals(1, retry.getRenewTimes());
        assertEquals(0, normal.getRenewTimes());
        completeSuccess(requestIndex("broker", ReceiptHandle.NORMAL_TOPIC));
    }

    @Test
    public void testFailedChunkDoesNotPreventLaterChunkCompletion() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        MessageReceiptHandle first = message("broker", ReceiptHandle.NORMAL_TOPIC, 0);
        MessageReceiptHandle last = message("broker", ReceiptHandle.NORMAL_TOPIC, 3);
        manager.renew(first, message("broker", ReceiptHandle.NORMAL_TOPIC, 1),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 2), last);
        responses.get(0).completeExceptionally(new TimeoutException("broker response lost"));
        assertEquals(2, requests.size());
        assertEquals(1, first.getRenewRetryTimes());
        assertEquals(first, manager.group.remove(first.getMessageId(), first.getOriginalReceiptHandleStr()));
        completeSuccess(1);
        assertEquals(1, last.getRenewTimes());
    }

    @Test
    public void testSynchronousSubmissionFailureReleasesAllChunks() {
        ConfigurationManager.getProxyConfig().setBatchChangeInvisibleTimeMaxNum(2);
        doAnswer(invocation -> {
            throw new IllegalStateException("request could not be submitted");
        }).when(messagingProcessor).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        MessageReceiptHandle first = message("broker", ReceiptHandle.NORMAL_TOPIC, 0);
        MessageReceiptHandle last = message("broker", ReceiptHandle.NORMAL_TOPIC, 3);
        manager.renew(first, message("broker", ReceiptHandle.NORMAL_TOPIC, 1),
            message("broker", ReceiptHandle.NORMAL_TOPIC, 2), last);
        assertEquals(2, events.size());
        assertTrue(events.get(0).getFuture().isCompletedExceptionally());
        assertTrue(events.get(1).getFuture().isCompletedExceptionally());
        assertEquals(1, first.getRenewRetryTimes());
        assertEquals(1, last.getRenewRetryTimes());
        assertEquals(first, manager.group.remove(first.getMessageId(), first.getOriginalReceiptHandleStr()));
        assertEquals(last, manager.group.remove(last.getMessageId(), last.getOriginalReceiptHandleStr()));
    }

    @Test
    public void testClearGroupBatchesByBrokerAndRealTopic() {
        manager.clear(message("fast", ReceiptHandle.NORMAL_TOPIC, 0), message("slow", ReceiptHandle.NORMAL_TOPIC, 1),
            message("fast", ReceiptHandle.NORMAL_TOPIC, 2), message("slow", ReceiptHandle.NORMAL_TOPIC, 3),
            message("fast", ReceiptHandle.RETRY_TOPIC_V2, 4), message("fast", ReceiptHandle.RETRY_TOPIC_V2, 5));
        assertEquals(3, requests.size());
        completeSuccess(requestIndex("fast", ReceiptHandle.NORMAL_TOPIC));
        assertEquals(1, events.stream().filter(event -> event.getFuture().isDone()).count());
        completeSuccess(requestIndex("fast", ReceiptHandle.RETRY_TOPIC_V2));
        completeSuccess(requestIndex("slow", ReceiptHandle.NORMAL_TOPIC));
        assertTrue(manager.group.isEmpty());
    }

    private class TestReceiptHandleManager extends DefaultReceiptHandleManager {
        private final ReceiptHandleGroup group = new ReceiptHandleGroup();
        private final ReceiptHandleGroupKey key = new ReceiptHandleGroupKey(new LocalChannel(), "group");

        TestReceiptHandleManager() {
            super(ReceiptHandleBatchCompletionTest.this.metadataService, ReceiptHandleBatchCompletionTest.this.consumerManager, event -> { },
                event -> {
                    events.add(event);
                    receiptProcessor.batchChangeInvisibleTime(ReceiptHandleBatchCompletionTest.createContext(), event);
                });
        }

        void renew(MessageReceiptHandle... messages) {
            List<RenewMessage> renewMessages = new ArrayList<>();
            for (MessageReceiptHandle message : messages) {
                group.put(message.getMessageId(), message);
                renewMessages.add(new RenewMessage(message.getMessageId(), message.getOriginalReceiptHandleStr(), message));
            }
            renewMessageBatch(createContext("test"), key, group, renewMessages);
        }

        void clear(MessageReceiptHandle... messages) {
            for (MessageReceiptHandle message : messages) {
                group.put(message.getMessageId(), message);
            }
            fireClearGroupEventBatch(key, group, ConfigurationManager.getProxyConfig());
        }
    }

    private MessageReceiptHandle message(String broker, String topicType, int index) {
        String handle = ReceiptHandle.builder().startOffset(0).retrieveTime(System.currentTimeMillis() - 60000
            + ConfigurationManager.getProxyConfig().getRenewAheadTimeMillis() - 5)
            .invisibleTime(60000).reviveQueueId(1).topicType(topicType).brokerName(broker)
            .queueId(0).offset(index).commitLogOffset(100 + index).build().encode();
        return new MessageReceiptHandle("group", "topic", 0, handle, "msg-" + index, index, 0);
    }

    private int requestIndex(String broker, String topicType) {
        for (int i = 0; i < requests.size(); i++) {
            ReceiptHandle handle = requests.get(i).get(0).getReceiptHandle();
            if (broker.equals(handle.getBrokerName()) && topicType.equals(handle.getTopicType())) {
                return i;
            }
        }
        throw new AssertionError("request not sent for " + broker + "/" + topicType);
    }

    private List<AckResult> successResults(List<ReceiptHandleMessage> messages) {
        List<AckResult> results = new ArrayList<>();
        for (ReceiptHandleMessage message : messages) {
            ReceiptHandle old = message.getReceiptHandle();
            AckResult result = new AckResult();
            result.setStatus(AckStatus.OK);
            result.setExtraInfo(ExtraInfoUtil.buildExtraInfo(old.getStartOffset(), System.currentTimeMillis(),
                message.getInvisibleTime(), old.getReviveQueueId(), old.getRealTopic("topic", "group"),
                old.getBrokerName(), old.getQueueId()) + MessageConst.KEY_SEPARATOR + old.getOffset());
            results.add(result);
        }
        return results;
    }

    private void completeSuccess(int index) {
        responses.get(index).complete(successResults(requests.get(index)));
    }
}
