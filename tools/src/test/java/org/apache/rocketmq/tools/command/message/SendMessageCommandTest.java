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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SendMessageCommandTest {

    private static SendMessageCommand sendMessageCommand;

    @BeforeClass
    public static void init() throws MQClientException, RemotingException, InterruptedException, MQBrokerException, NoSuchFieldException, IllegalAccessException {

        DefaultMQProducer defaultMQProducer = mock(DefaultMQProducer.class);
        SendResult sendResult = new SendResult();
        sendResult.setMessageQueue(new MessageQueue());
        sendResult.getMessageQueue().setBrokerName("broker1");
        sendResult.getMessageQueue().setQueueId(1);
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("fgwejigherughwueyutyu4t4343t43");

        when(defaultMQProducer.send(any(Message.class))).thenReturn(sendResult);
        when(defaultMQProducer.send(any(Message.class), any(MessageQueue.class))).thenReturn(sendResult);

        sendMessageCommand = commandWithProducer(defaultMQProducer);
    }

    @AfterClass
    public static void terminate() {
    }

    @Test
    public void testExecute() throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t mytopic","-p 'send message test'","-c tagA","-k order-16546745756"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + sendMessageCommand.commandName(),
            subargs, sendMessageCommand.buildCommandlineOptions(options), new DefaultParser());
        sendMessageCommand.execute(commandLine, options, null);

        subargs = new String[] {"-t mytopic","-p 'send message test'","-c tagA","-k order-16546745756","-b brokera","-i 1"};
        commandLine = ServerUtil.parseCmdLine("mqadmin " + sendMessageCommand.commandName(), subargs,
            sendMessageCommand.buildCommandlineOptions(options), new DefaultParser());
        sendMessageCommand.execute(commandLine, options, null);
        System.setOut(out);
        String s = new String(bos.toByteArray());
        Assert.assertTrue(s.contains("SEND_OK"));
    }

    @Test
    public void testExecutePreservesBodyWhitespace() throws Exception {
        assertMessageBody("\t message \r\n", false);
    }

    @Test
    public void testExecutePreservesWhitespaceOnlyBody() throws Exception {
        assertMessageBody("   ", false);
    }

    @Test
    public void testExecutePreservesBodyWhenQueueIsSpecified() throws Exception {
        assertMessageBody("  \u6d88\u606f\u6b63\u6587  ", true);
    }

    @Test
    public void testExecutePreservesOrdinaryBody() throws Exception {
        assertMessageBody("message body", false);
    }

    @Test
    public void testExecuteSelectsWritableQueueOnRequestedBroker() throws Exception {
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        MessageQueue first = new MessageQueue("mytopic", "broker-a", 0);
        MessageQueue second = new MessageQueue("mytopic", "broker-a", 1);
        MessageQueue otherBroker = new MessageQueue("mytopic", "broker-b", 0);
        when(producer.fetchPublishMessageQueues("mytopic")).thenReturn(Arrays.asList(otherBroker, first, second));
        when(producer.send(any(Message.class), any(MessageQueue.class))).thenAnswer(invocation ->
            new SendResult(SendStatus.SEND_OK, "message-id", "offset-id", invocation.getArgument(1), 0));

        executeWithTargetOptions(producer, "-b", "broker-a");

        ArgumentCaptor<MessageQueue> queue = ArgumentCaptor.forClass(MessageQueue.class);
        verify(producer).send(any(Message.class), queue.capture());
        Assert.assertTrue(Arrays.asList(first, second).contains(queue.getValue()));
        verify(producer, never()).send(any(Message.class));
    }

    @Test
    public void testExecuteRejectsBrokerWithoutWritableQueue() throws Exception {
        assertNoQueueOnBroker("broker-a", new MessageQueue("mytopic", "broker-b", 0));
    }

    @Test
    public void testExecuteRejectsLogicalQueueInBrokerOnlyMode() throws Exception {
        String logicalBroker = MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX + "scope";
        assertNoQueueOnBroker(logicalBroker, new MessageQueue("mytopic", logicalBroker, 0));
    }

    @Test
    public void testExecuteDoesNotFallBackWhenTargetQueueSendFails() throws Exception {
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        MessageQueue queue = new MessageQueue("mytopic", "broker-a", 0);
        when(producer.fetchPublishMessageQueues("mytopic")).thenReturn(Collections.singletonList(queue));
        MQClientException failure = new MQClientException("Target broker unavailable", null);
        when(producer.send(any(Message.class), eq(queue))).thenThrow(failure);

        RuntimeException error = Assert.assertThrows(RuntimeException.class,
            () -> executeWithTargetOptions(producer, "-b", "broker-a"));

        Assert.assertSame(failure, error.getCause());
        verify(producer, never()).send(any(Message.class));
        verify(producer).shutdown();
    }

    @Test
    public void testExecuteRejectsQueueWithoutBroker() throws Exception {
        assertInvalidTarget("requires -b", "-i", "0");
    }

    @Test
    public void testExecuteRejectsInvalidTargetQueue() throws Exception {
        assertInvalidTarget("must be a non-negative integer", "-b", "broker-a", "-i", "-1");
        assertInvalidTarget("must be a non-negative integer", "-b", "broker-a", "-i", "invalid");
        assertInvalidTarget("must specify a broker name", "-b", " ");
    }

    private void assertNoQueueOnBroker(String broker, MessageQueue availableQueue) throws Exception {
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        when(producer.fetchPublishMessageQueues("mytopic")).thenReturn(Collections.singletonList(availableQueue));

        SubCommandException error = Assert.assertThrows(SubCommandException.class,
            () -> executeWithTargetOptions(producer, "-b", broker));

        Assert.assertTrue(error.getMessage(), error.getMessage().contains("No writable queue for topic mytopic on broker " + broker));
        verify(producer, never()).send(any(Message.class));
        verify(producer, never()).send(any(Message.class), any(MessageQueue.class));
        verify(producer).shutdown();
    }

    private void assertInvalidTarget(String expectedError, String... targetOptions) throws Exception {
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        SubCommandException error = Assert.assertThrows(SubCommandException.class,
            () -> executeWithTargetOptions(producer, targetOptions));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains(expectedError));
        verifyNoInteractions(producer);
    }

    private void executeWithTargetOptions(DefaultMQProducer producer, String... targetOptions) throws Exception {
        SendMessageCommand command = commandWithProducer(producer);
        Options options = command.buildCommandlineOptions(new Options());
        String[] arguments = new String[4 + targetOptions.length];
        System.arraycopy(new String[] {"-t", "mytopic", "-p", "message body"}, 0, arguments, 0, 4);
        System.arraycopy(targetOptions, 0, arguments, 4, targetOptions.length);
        CommandLine commandLine = new DefaultParser().parse(options, arguments);
        command.execute(commandLine, options, null);
    }

    private void assertMessageBody(String body, boolean sendToQueue) throws Exception {
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        SendMessageCommand command = commandWithProducer(producer);
        MessageQueue queue = new MessageQueue("mytopic", "broker-a", 1);
        SendResult result = new SendResult(SendStatus.SEND_OK, "message-id", "offset-id", queue, 0);
        when(producer.send(any(Message.class))).thenReturn(result);
        when(producer.send(any(Message.class), any(MessageQueue.class))).thenReturn(result);
        Options options = command.buildCommandlineOptions(new Options());
        String[] arguments = sendToQueue
            ? new String[] {"-t", "mytopic", "-p", body, "-b", "broker-a", "-i", "1"}
            : new String[] {"-t", "mytopic", "-p", body};
        CommandLine commandLine = new DefaultParser().parse(options, arguments);
        command.execute(commandLine, options, null);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        if (sendToQueue) {
            verify(producer).send(messageCaptor.capture(), eq(queue));
        } else {
            verify(producer).send(messageCaptor.capture());
        }
        Assert.assertArrayEquals(body.getBytes(StandardCharsets.UTF_8), messageCaptor.getValue().getBody());
    }

    private static SendMessageCommand commandWithProducer(DefaultMQProducer producer)
        throws NoSuchFieldException, IllegalAccessException {
        SendMessageCommand command = new SendMessageCommand();
        Field producerField = SendMessageCommand.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(command, producer);
        return command;
    }
}
