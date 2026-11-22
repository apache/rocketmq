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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.junit.Assert;
import org.junit.Test;

public class PrintMessageByQueueCommandTest {

    @Test
    public void testCompareToIsAntisymmetricBeyondIntegerMaxValue() {
        PrintMessageByQueueCommand.TagCountBean zero = newTagCountBean("tag-zero", 0L);
        PrintMessageByQueueCommand.TagCountBean overInt = newTagCountBean("tag-over-int", (long) Integer.MAX_VALUE + 1L);

        // (int) (o.count - this.count) wraps for this pair and returns a negative value in
        // both directions, so assert the sign of the comparison is properly inverted.
        Assert.assertTrue(zero.compareTo(overInt) > 0);
        Assert.assertTrue(overInt.compareTo(zero) < 0);
        Assert.assertEquals(Integer.signum(overInt.compareTo(zero)), -Integer.signum(zero.compareTo(overInt)));
    }

    @Test
    public void testCollectionsSortOrdersTagCountsDescending() {
        PrintMessageByQueueCommand.TagCountBean zero = newTagCountBean("tag-zero", 0L);
        PrintMessageByQueueCommand.TagCountBean overInt = newTagCountBean("tag-over-int", (long) Integer.MAX_VALUE + 1L);
        PrintMessageByQueueCommand.TagCountBean max = newTagCountBean("tag-max", Long.MAX_VALUE);

        List<PrintMessageByQueueCommand.TagCountBean> beans = new ArrayList<>();
        beans.add(zero);
        beans.add(max);
        beans.add(overInt);
        Collections.sort(beans);

        Assert.assertEquals(Arrays.asList("tag-max", "tag-over-int", "tag-zero"), tagsOf(beans));
    }

    @Test
    public void testEqualCountsCompareEqual() {
        PrintMessageByQueueCommand.TagCountBean first = newTagCountBean("tag-first", (long) Integer.MAX_VALUE + 1L);
        PrintMessageByQueueCommand.TagCountBean second = newTagCountBean("tag-second", (long) Integer.MAX_VALUE + 1L);

        // Equal counts must produce an equal comparison in both directions, otherwise
        // TimSort treats the two entries as an inconsistent pair.
        Assert.assertEquals(0, first.compareTo(second));
        Assert.assertEquals(0, second.compareTo(first));
    }

    @Test
    public void testCompareToHandlesLongMaxValueCounts() {
        PrintMessageByQueueCommand.TagCountBean overInt = newTagCountBean("tag-over-int", (long) Integer.MAX_VALUE + 1L);
        PrintMessageByQueueCommand.TagCountBean max = newTagCountBean("tag-max", Long.MAX_VALUE);

        // The largest possible gap between two long counters must still order correctly.
        Assert.assertTrue(overInt.compareTo(max) > 0);
        Assert.assertTrue(max.compareTo(overInt) < 0);
    }

    @Test
    public void testTagScanContinuesAfterUnmatchedBatch() throws Exception {
        MessageExt matched = new MessageExt();
        matched.setTags("TagA");
        ScriptedConsumer consumer = new ScriptedConsumer(
            new PullResult(PullStatus.NO_MATCHED_MSG, 32, 0, 33, Collections.emptyList()),
            new PullResult(PullStatus.FOUND, 33, 0, 33, Collections.singletonList(matched)));

        String output = runTagScan(consumer);

        Assert.assertEquals(Arrays.asList(0L, 32L), consumer.requestedOffsets);
        Assert.assertTrue(output.replaceAll("\\s+", " ").contains("Tag: TagA Count: 1"));
    }

    @Test
    public void testTagScanStopsWhenUnmatchedBatchDoesNotAdvance() throws Exception {
        ScriptedConsumer consumer = new ScriptedConsumer(
            new PullResult(PullStatus.NO_MATCHED_MSG, 0, 0, 33, Collections.emptyList()));

        runTagScan(consumer);

        Assert.assertEquals(Collections.singletonList(0L), consumer.requestedOffsets);
    }

    private static String runTagScan(ScriptedConsumer consumer) throws Exception {
        PrintMessageByQueueCommand command = new PrintMessageByQueueCommand() {
            @Override
            DefaultMQPullConsumer createConsumer(RPCHook rpcHook) {
                return consumer;
            }
        };
        Options options = command.buildCommandlineOptions(new Options());
        CommandLine arguments = new DefaultParser().parse(options,
            new String[] {"-t", "TestTopic", "-a", "broker-a", "-i", "0", "-s", "TagA", "-f", "true"});
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try (PrintStream captured = new PrintStream(output, true, "UTF-8")) {
            System.setOut(captured);
            command.execute(arguments, options, null);
        } finally {
            System.setOut(original);
        }
        return output.toString("UTF-8");
    }

    private static class ScriptedConsumer extends DefaultMQPullConsumer {
        private final PullResult[] results;
        private final List<Long> requestedOffsets = new ArrayList<>();

        ScriptedConsumer(PullResult... results) {
            this.results = results;
        }

        @Override
        public void start() {
            // The responses are scripted, so no client connection is needed.
        }

        @Override
        public void shutdown() {
            // No client connection was opened.
        }

        @Override
        public long minOffset(MessageQueue queue) {
            return 0;
        }

        @Override
        public long maxOffset(MessageQueue queue) {
            return 33;
        }

        @Override
        public PullResult pull(MessageQueue queue, String expression, long offset, int maxNums) {
            Assert.assertEquals("TagA", expression);
            int resultIndex = requestedOffsets.size();
            requestedOffsets.add(offset);
            Assert.assertTrue("Unexpected pull at offset " + offset, resultIndex < results.length);
            return results[resultIndex];
        }
    }

    private static PrintMessageByQueueCommand.TagCountBean newTagCountBean(String tag, long count) {
        return new PrintMessageByQueueCommand.TagCountBean(tag, new AtomicLong(count));
    }

    private static List<String> tagsOf(List<PrintMessageByQueueCommand.TagCountBean> beans) {
        List<String> tags = new ArrayList<>(beans.size());
        for (PrintMessageByQueueCommand.TagCountBean bean : beans) {
            tags.add(bean.getTag());
        }
        return tags;
    }
}
