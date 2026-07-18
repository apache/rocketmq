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
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class PushConsumeMessageCommandTest {

    @Test
    public void testCommandMetadataAndOptions() {
        PushConsumeMessageCommand command = new PushConsumeMessageCommand();
        Options options = command.buildCommandlineOptions(new Options());

        Assert.assertEquals("consumeMessageByPush", command.commandName());
        Assert.assertTrue(command.commandDesc().contains("PushConsumer"));
        Assert.assertTrue(options.getOption("topic").isRequired());
        Assert.assertTrue(options.getOption("consumerGroup").isRequired());
        Assert.assertNotNull(options.getOption("messageCount"));
        Assert.assertNotNull(options.getOption("maxWaitSeconds"));
        Assert.assertNotNull(options.getOption("messageTraceEnabled"));
        Assert.assertNotNull(options.getOption("consumeTimestamp"));
    }

    @Test
    public void testConcurrentConsumerConfigurationAndDelivery() throws Exception {
        DefaultMQPushConsumer consumer = mock(DefaultMQPushConsumer.class);
        AtomicReference<MessageListenerConcurrently> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(consumer).registerMessageListener(any(MessageListenerConcurrently.class));
        doAnswer(invocation -> {
            MessageQueue queue = new MessageQueue("TopicA", "broker-a", 2);
            ConsumeConcurrentlyStatus status = listener.get().consumeMessage(
                Collections.singletonList(message()), new ConsumeConcurrentlyContext(queue));
            Assert.assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, status);
            return null;
        }).when(consumer).start();

        FactoryCapture factory = new FactoryCapture(consumer);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PushConsumeMessageCommand command = new PushConsumeMessageCommand(factory, new PrintStream(output));
        CommandLine commandLine = parse(command,
            "-t", "TopicA", "-g", "GroupA", "-s", "TagA", "-c", "1", "-b", "16",
            "-i", "diagnostic", "-f", "first", "-d", "true", "-p", "true");

        command.execute(commandLine, new Options(), null);

        Assert.assertEquals(1, factory.calls.get());
        Assert.assertEquals("GroupA", factory.consumerGroup.get());
        Assert.assertFalse(factory.traceEnabled);
        verify(consumer).setInstanceName("diagnostic");
        verify(consumer).setConsumeMessageBatchMaxSize(16);
        verify(consumer).setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        verify(consumer, never()).setConsumeTimestamp(anyString());
        verify(consumer).subscribe("TopicA", "TagA");
        verify(consumer).start();
        verify(consumer).shutdown();
        String text = new String(output.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertTrue(text.contains("Push consumer started"));
        Assert.assertTrue(text.contains("body=payload"));
        Assert.assertTrue(text.contains("reason=MESSAGE_LIMIT, messages=1"));
    }

    @Test
    public void testOrderlyConsumerAndTraceConfiguration() throws Exception {
        DefaultMQPushConsumer consumer = mock(DefaultMQPushConsumer.class);
        AtomicReference<MessageListenerOrderly> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(consumer).registerMessageListener(any(MessageListenerOrderly.class));
        doAnswer(invocation -> {
            MessageQueue queue = new MessageQueue("TopicA", "broker-a", 2);
            ConsumeOrderlyStatus status = listener.get().consumeMessage(Collections.singletonList(message()),
                new ConsumeOrderlyContext(queue));
            Assert.assertEquals(ConsumeOrderlyStatus.SUCCESS, status);
            return null;
        }).when(consumer).start();

        FactoryCapture factory = new FactoryCapture(consumer);
        PushConsumeMessageCommand command = new PushConsumeMessageCommand(factory,
            new PrintStream(new ByteArrayOutputStream()));
        CommandLine commandLine = parse(command, "-t", "TopicA", "-g", "GroupA", "-c", "1", "-o", "true",
            "-m", "true", "-r", "TraceTopic", "-f", "timestamp", "-x", "20260718120000");

        command.execute(commandLine, new Options(), null);

        Assert.assertTrue(factory.traceEnabled);
        Assert.assertEquals("TraceTopic", factory.traceTopic.get());
        verify(consumer).setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        verify(consumer).setConsumeTimestamp("20260718120000");
        verify(consumer).registerMessageListener(any(MessageListenerOrderly.class));
        verify(consumer, never()).registerMessageListener(any(MessageListenerConcurrently.class));
        verify(consumer).shutdown();
    }

    @Test
    public void testInvalidArgumentsDoNotCreateConsumer() throws Exception {
        FactoryCapture factory = new FactoryCapture(mock(DefaultMQPushConsumer.class));
        PushConsumeMessageCommand command = new PushConsumeMessageCommand(factory,
            new PrintStream(new ByteArrayOutputStream()));
        CommandLine commandLine = parse(command, "-t", "TopicA", "-g", "GroupA", "-b", "0");

        SubCommandException exception = Assert.assertThrows(SubCommandException.class,
            () -> command.execute(commandLine, new Options(), null));

        Assert.assertTrue(exception.getMessage().contains("invalid arguments"));
        Assert.assertEquals(0, factory.calls.get());
    }

    @Test
    public void testStartFailureStillShutsDownAndPrintsSummary() throws Exception {
        DefaultMQPushConsumer consumer = mock(DefaultMQPushConsumer.class);
        doThrow(new IllegalStateException("start failed")).when(consumer).start();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PushConsumeMessageCommand command = new PushConsumeMessageCommand(new FactoryCapture(consumer),
            new PrintStream(output));
        CommandLine commandLine = parse(command, "-t", "TopicA", "-g", "GroupA", "-c", "1");

        SubCommandException exception = Assert.assertThrows(SubCommandException.class,
            () -> command.execute(commandLine, new Options(), null));

        Assert.assertTrue(exception.getMessage().contains("command failed"));
        verify(consumer).shutdown();
        Assert.assertTrue(new String(output.toByteArray(), StandardCharsets.UTF_8)
            .contains("No messages were received"));
    }

    @Test
    public void testInterruptedWaitRestoresInterruptFlag() throws Exception {
        DefaultMQPushConsumer consumer = mock(DefaultMQPushConsumer.class);
        PushConsumeMessageCommand command = new PushConsumeMessageCommand(new FactoryCapture(consumer),
            new PrintStream(new ByteArrayOutputStream()));
        CommandLine commandLine = parse(command, "-t", "TopicA", "-g", "GroupA");

        Thread.currentThread().interrupt();
        try {
            command.execute(commandLine, new Options(), null);
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        verify(consumer).shutdown();
    }

    @Test
    public void testRpcHookPassedToFactory() throws Exception {
        DefaultMQPushConsumer consumer = mock(DefaultMQPushConsumer.class);
        AtomicReference<MessageListenerConcurrently> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return null;
        }).when(consumer).registerMessageListener(any(MessageListenerConcurrently.class));
        doAnswer(invocation -> {
            listener.get().consumeMessage(Collections.singletonList(message()),
                new ConsumeConcurrentlyContext(new MessageQueue("TopicA", "broker-a", 2)));
            return null;
        }).when(consumer).start();
        FactoryCapture factory = new FactoryCapture(consumer);
        PushConsumeMessageCommand command = new PushConsumeMessageCommand(factory,
            new PrintStream(new ByteArrayOutputStream()));
        RPCHook rpcHook = mock(RPCHook.class);

        command.execute(parse(command, "-t", "TopicA", "-g", "GroupA", "-c", "1"), new Options(), rpcHook);

        Assert.assertSame(rpcHook, factory.rpcHook.get());
    }

    private CommandLine parse(PushConsumeMessageCommand command, String... arguments) throws Exception {
        Options options = command.buildCommandlineOptions(new Options());
        return new DefaultParser().parse(options, arguments);
    }

    private MessageExt message() {
        MessageExt message = new MessageExt();
        message.setTopic("TopicA");
        message.setBrokerName("broker-a");
        message.setQueueId(2);
        message.setQueueOffset(10);
        message.setMsgId("message-id");
        message.setBody("payload".getBytes(StandardCharsets.UTF_8));
        message.setBornTimestamp(100);
        message.setStoreTimestamp(200);
        message.setBornHost(new InetSocketAddress("127.0.0.1", 1000));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 2000));
        return message;
    }

    private static class FactoryCapture implements PushConsumeMessageCommand.ConsumerFactory {
        private final DefaultMQPushConsumer consumer;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicReference<String> consumerGroup = new AtomicReference<>();
        private final AtomicReference<RPCHook> rpcHook = new AtomicReference<>();
        private final AtomicReference<AllocateMessageQueueStrategy> allocationStrategy = new AtomicReference<>();
        private final AtomicReference<String> traceTopic = new AtomicReference<>();
        private boolean traceEnabled;

        private FactoryCapture(DefaultMQPushConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public DefaultMQPushConsumer create(String consumerGroup, RPCHook rpcHook,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMessageTrace,
            String customizedTraceTopic) {
            calls.incrementAndGet();
            this.consumerGroup.set(consumerGroup);
            this.rpcHook.set(rpcHook);
            this.allocationStrategy.set(allocateMessageQueueStrategy);
            this.traceEnabled = enableMessageTrace;
            this.traceTopic.set(customizedTraceTopic);
            return consumer;
        }
    }
}
