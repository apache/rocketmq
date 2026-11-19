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
package org.apache.rocketmq.tools.command.message;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.Test;

public class PushConsumeMessageObserverTest {

    @Test
    public void testConcurrentMessagesRecordedByQueue() throws Exception {
        TestContext context = createContext(2, 1_000);
        MessageQueue queue = new MessageQueue("TopicA", "broker-a", 1);
        MessageExt first = message("TopicA", "broker-a", 1, 10, "one");
        MessageExt second = message("TopicA", "broker-a", 1, 11, "second");

        ConsumeConcurrentlyStatus status = context.observer.consumeMessage(Arrays.asList(first, second),
            new ConsumeConcurrentlyContext(queue));

        Assert.assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, status);
        Assert.assertEquals(PushConsumeMessageObserver.CompletionReason.MESSAGE_LIMIT,
            context.observer.awaitCompletion());
        PushConsumeMessageObserver.Snapshot snapshot = context.observer.snapshot();
        Assert.assertEquals(2, snapshot.getMessageCount());
        Assert.assertEquals(9, snapshot.getBodyBytes());
        Assert.assertEquals(1, snapshot.getQueueSnapshots().size());
        Assert.assertEquals(2, snapshot.getQueueSnapshots().get(0).getMessageCount());
        Assert.assertEquals(10, snapshot.getQueueSnapshots().get(0).getFirstOffset());
        Assert.assertEquals(11, snapshot.getQueueSnapshots().get(0).getLastOffset());
        Assert.assertTrue(snapshot.getFirstReceiveTimestamp() > 0);
        Assert.assertTrue(snapshot.getLastReceiveTimestamp() >= snapshot.getFirstReceiveTimestamp());
    }

    @Test
    public void testOrderlyMessageReturnsSuccess() throws Exception {
        TestContext context = createContext(1, 1_000);
        MessageQueue queue = new MessageQueue("TopicA", "broker-a", 0);

        ConsumeOrderlyStatus status = context.observer.consumeMessage(
            Collections.singletonList(message("TopicA", "broker-a", 0, 3, "body")),
            new ConsumeOrderlyContext(queue));

        Assert.assertEquals(ConsumeOrderlyStatus.SUCCESS, status);
        Assert.assertEquals(PushConsumeMessageObserver.CompletionReason.MESSAGE_LIMIT,
            context.observer.awaitCompletion());
    }

    @Test
    public void testEmptyBatchDoesNotChangeSnapshot() {
        TestContext context = createContext(1, 1_000);
        context.observer.record(Collections.emptyList(), new MessageQueue("TopicA", "broker-a", 0));

        PushConsumeMessageObserver.Snapshot snapshot = context.observer.snapshot();
        Assert.assertEquals(0, snapshot.getMessageCount());
        Assert.assertEquals(0, snapshot.getBodyBytes());
        Assert.assertTrue(snapshot.getQueueSnapshots().isEmpty());
    }

    @Test
    public void testNullBatchDoesNotChangeSnapshot() {
        TestContext context = createContext(1, 1_000);
        context.observer.record(null, new MessageQueue("TopicA", "broker-a", 0));
        Assert.assertEquals(0, context.observer.snapshot().getMessageCount());
    }

    @Test
    public void testNullBodyDoesNotIncrementBytes() {
        TestContext context = createContext(1, 1_000);
        MessageExt message = message("TopicA", "broker-a", 0, 4, "body");
        message.setBody(null);

        context.observer.record(Collections.singletonList(message), null);

        Assert.assertEquals(1, context.observer.snapshot().getMessageCount());
        Assert.assertEquals(0, context.observer.snapshot().getBodyBytes());
    }

    @Test
    public void testDeliveryQueuePreferredOverMessageMetadata() {
        TestContext context = createContext(1, 1_000);
        MessageExt message = message("TopicA", "message-broker", 7, 4, "body");
        MessageQueue deliveryQueue = new MessageQueue("TopicA", "delivery-broker", 2);

        context.observer.record(Collections.singletonList(message), deliveryQueue);

        MessageQueue recordedQueue = context.observer.snapshot().getQueueSnapshots().get(0).getMessageQueue();
        Assert.assertEquals("delivery-broker", recordedQueue.getBrokerName());
        Assert.assertEquals(2, recordedQueue.getQueueId());
    }

    @Test
    public void testQueueMetadataFallsBackToMessage() {
        TestContext context = createContext(1, 1_000);
        context.observer.record(Collections.singletonList(message("TopicA", "broker-a", 3, 8, "body")), null);

        MessageQueue recordedQueue = context.observer.snapshot().getQueueSnapshots().get(0).getMessageQueue();
        Assert.assertEquals("TopicA", recordedQueue.getTopic());
        Assert.assertEquals("broker-a", recordedQueue.getBrokerName());
        Assert.assertEquals(3, recordedQueue.getQueueId());
    }

    @Test
    public void testQueueSnapshotsAreSorted() {
        TestContext context = createContext(3, 1_000);
        context.observer.record(Collections.singletonList(message("TopicA", "broker-b", 1, 1, "a")), null);
        context.observer.record(Collections.singletonList(message("TopicA", "broker-a", 2, 1, "b")), null);
        context.observer.record(Collections.singletonList(message("TopicA", "broker-a", 0, 1, "c")), null);

        Assert.assertEquals("broker-a", context.observer.snapshot().getQueueSnapshots().get(0)
            .getMessageQueue().getBrokerName());
        Assert.assertEquals(0, context.observer.snapshot().getQueueSnapshots().get(0)
            .getMessageQueue().getQueueId());
        Assert.assertEquals(2, context.observer.snapshot().getQueueSnapshots().get(1)
            .getMessageQueue().getQueueId());
        Assert.assertEquals("broker-b", context.observer.snapshot().getQueueSnapshots().get(2)
            .getMessageQueue().getBrokerName());
    }

    @Test
    public void testTimeLimitCompletion() throws Exception {
        TestContext context = createContext(5, 5);
        Assert.assertEquals(PushConsumeMessageObserver.CompletionReason.TIME_LIMIT,
            context.observer.awaitCompletion());
    }

    @Test
    public void testStopUnblocksUnlimitedObserver() throws Exception {
        TestContext context = createContext(0, 0);
        CompletableFuture<PushConsumeMessageObserver.CompletionReason> result = CompletableFuture.supplyAsync(() -> {
            try {
                return context.observer.awaitCompletion();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });

        context.observer.stop();

        Assert.assertEquals(PushConsumeMessageObserver.CompletionReason.STOPPED,
            result.get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testMessageLimitCanBeExceededByBatch() throws Exception {
        TestContext context = createContext(1, 1_000);
        context.observer.record(Arrays.asList(
            message("TopicA", "broker-a", 0, 1, "a"),
            message("TopicA", "broker-a", 0, 2, "b")), null);

        Assert.assertEquals(PushConsumeMessageObserver.CompletionReason.MESSAGE_LIMIT,
            context.observer.awaitCompletion());
        Assert.assertEquals(2, context.observer.snapshot().getMessageCount());
    }

    private TestContext createContext(long maxMessages, long maxWaitMillis) {
        PushConsumeMessageConfig config = new PushConsumeMessageConfig("TopicA", "GroupA", "*", "instance",
            maxMessages, maxWaitMillis, 1, false, false, null, true, false, StandardCharsets.UTF_8,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, null);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PushConsumeMessagePrinter printer = new PushConsumeMessagePrinter(config, new PrintStream(output));
        return new TestContext(new PushConsumeMessageObserver(config, printer), output);
    }

    private MessageExt message(String topic, String brokerName, int queueId, long queueOffset, String body) {
        MessageExt message = new MessageExt();
        message.setTopic(topic);
        message.setBrokerName(brokerName);
        message.setQueueId(queueId);
        message.setQueueOffset(queueOffset);
        message.setMsgId("msg-" + queueOffset);
        message.setBody(body.getBytes(StandardCharsets.UTF_8));
        message.setBornTimestamp(100);
        message.setStoreTimestamp(200 + queueOffset);
        message.setBornHost(new InetSocketAddress("127.0.0.1", 1000));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 2000));
        return message;
    }

    private static class TestContext {
        private final PushConsumeMessageObserver observer;
        private final ByteArrayOutputStream output;

        private TestContext(PushConsumeMessageObserver observer, ByteArrayOutputStream output) {
            this.observer = observer;
            this.output = output;
        }
    }
}
