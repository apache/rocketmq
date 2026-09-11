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
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.Test;

public class PushConsumeMessagePrinterTest {

    @Test
    public void testMessageDetailsAndBodyPrinted() {
        TestPrinter testPrinter = createPrinter(true, true, 5, 3_000);
        MessageExt message = message();
        message.putUserProperty("z-user", "last");
        message.putUserProperty("a-user", "first");

        testPrinter.printer.printMessage(1, 300, new MessageQueue("TopicA", "broker-a", 2), message);

        String output = testPrinter.output();
        Assert.assertTrue(output.contains("#1 receiveTime="));
        Assert.assertTrue(output.contains("broker=broker-a queueId=2 queueOffset=12"));
        Assert.assertTrue(output.contains("msgId=message-id keys=key-a tags=TagA reconsumeTimes=3 bodySize=5"));
        Assert.assertTrue(output.contains("bornHost=127.0.0.1:1000"));
        Assert.assertTrue(output.contains("storeHost=127.0.0.1:2000"));
        Assert.assertTrue(output.contains("a-user=first"));
        Assert.assertTrue(output.contains("z-user=last"));
        Assert.assertTrue(output.contains("body=hello"));
    }

    @Test
    public void testBodyAndPropertiesCanBeHidden() {
        TestPrinter testPrinter = createPrinter(false, false, 1, 1_000);
        testPrinter.printer.printMessage(1, 300, new MessageQueue("TopicA", "broker-a", 2), message());
        Assert.assertFalse(testPrinter.output().contains("body="));
        Assert.assertFalse(testPrinter.output().contains("properties="));
    }

    @Test
    public void testNullBodyPrinted() {
        TestPrinter testPrinter = createPrinter(true, false, 1, 1_000);
        MessageExt message = message();
        message.setBody(null);
        testPrinter.printer.printMessage(1, 300, new MessageQueue("TopicA", "broker-a", 2), message);
        Assert.assertTrue(testPrinter.output().contains("body=<null>"));
    }

    @Test
    public void testEmptyPropertiesPrinted() {
        TestPrinter testPrinter = createPrinter(false, true, 1, 1_000);
        MessageExt message = new MessageExt();
        message.setMsgId("message-id");
        message.setQueueOffset(12);
        message.setBornHost(new InetSocketAddress("127.0.0.1", 1000));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 2000));
        testPrinter.printer.printMessage(1, 300, new MessageQueue("TopicA", "broker-a", 2), message);
        Assert.assertTrue(testPrinter.output().contains("properties={}"));
    }

    @Test
    public void testStartedOutputForBoundedRun() {
        TestPrinter testPrinter = createPrinter(true, false, 5, 3_000);
        testPrinter.printer.printStarted();

        Assert.assertTrue(testPrinter.output().contains("Push consumer started. topic=TopicA, group=GroupA"));
        Assert.assertTrue(testPrinter.output().contains("Stop condition: messageCount=5, maxWaitMillis=3000"));
    }

    @Test
    public void testStartedOutputForUnlimitedRun() {
        TestPrinter testPrinter = createPrinter(true, false, 0, 0);
        testPrinter.printer.printStarted();
        Assert.assertTrue(testPrinter.output().contains("keep consuming until the process is interrupted"));
    }

    @Test
    public void testEmptySummary() {
        TestPrinter testPrinter = createPrinter(true, false, 1, 1_000);
        PushConsumeMessageObserver.Snapshot snapshot = new PushConsumeMessageObserver.Snapshot(0, 0, 0, 0,
            Collections.emptyList());

        testPrinter.printer.printSummary(snapshot, PushConsumeMessageObserver.CompletionReason.TIME_LIMIT);

        Assert.assertTrue(testPrinter.output().contains("reason=TIME_LIMIT, messages=0, bodyBytes=0"));
        Assert.assertTrue(testPrinter.output().contains("No messages were received"));
    }

    @Test
    public void testQueueSummary() {
        TestPrinter testPrinter = createPrinter(true, false, 1, 1_000);
        PushConsumeMessageObserver.QueueSnapshot queue = new PushConsumeMessageObserver.QueueSnapshot(
            new MessageQueue("TopicA", "broker-a", 2), 4, 10, 13, 500);
        PushConsumeMessageObserver.Snapshot snapshot = new PushConsumeMessageObserver.Snapshot(4, 20, 100, 250,
            Collections.singletonList(queue));

        testPrinter.printer.printSummary(snapshot, PushConsumeMessageObserver.CompletionReason.MESSAGE_LIMIT);

        String output = testPrinter.output();
        Assert.assertTrue(output.contains("reason=MESSAGE_LIMIT, messages=4, bodyBytes=20, elapsedMillis=150"));
        Assert.assertTrue(output.contains("broker-a"));
        Assert.assertTrue(output.contains("10"));
        Assert.assertTrue(output.contains("13"));
    }

    private TestPrinter createPrinter(boolean printBody, boolean printProperties, long maxMessages,
        long maxWaitMillis) {
        PushConsumeMessageConfig config = new PushConsumeMessageConfig("TopicA", "GroupA", "*", "instance",
            maxMessages, maxWaitMillis, 1, false, false, null, printBody, printProperties, StandardCharsets.UTF_8,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, null);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        return new TestPrinter(new PushConsumeMessagePrinter(config, new PrintStream(output)), output);
    }

    private MessageExt message() {
        MessageExt message = new MessageExt();
        message.setTopic("TopicA");
        message.setBrokerName("broker-a");
        message.setQueueId(2);
        message.setQueueOffset(12);
        message.setMsgId("message-id");
        message.setKeys("key-a");
        message.setTags("TagA");
        message.setReconsumeTimes(3);
        message.setBody("hello".getBytes(StandardCharsets.UTF_8));
        message.setBornTimestamp(100);
        message.setStoreTimestamp(200);
        message.setBornHost(new InetSocketAddress("127.0.0.1", 1000));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 2000));
        return message;
    }

    private static class TestPrinter {
        private final PushConsumeMessagePrinter printer;
        private final ByteArrayOutputStream output;

        private TestPrinter(PushConsumeMessagePrinter printer, ByteArrayOutputStream output) {
            this.printer = printer;
            this.output = output;
        }

        private String output() {
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
