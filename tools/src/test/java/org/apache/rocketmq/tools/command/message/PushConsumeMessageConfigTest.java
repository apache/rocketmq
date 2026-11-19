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

import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.junit.Assert;
import org.junit.Test;

public class PushConsumeMessageConfigTest {

    @Test
    public void testDefaultConfiguration() throws Exception {
        PushConsumeMessageConfig config = parse("-t", "TopicA", "-g", "GroupA");

        Assert.assertEquals("TopicA", config.getTopic());
        Assert.assertEquals("GroupA", config.getConsumerGroup());
        Assert.assertEquals("*", config.getSubExpression());
        Assert.assertTrue(config.getInstanceName().startsWith("mqadmin_push_"));
        Assert.assertEquals(0, config.getMaxMessages());
        Assert.assertEquals(0, config.getMaxWaitMillis());
        Assert.assertEquals(1, config.getBatchSize());
        Assert.assertFalse(config.isOrderly());
        Assert.assertFalse(config.isMessageTraceEnabled());
        Assert.assertNull(config.getCustomizedTraceTopic());
        Assert.assertTrue(config.isPrintBody());
        Assert.assertFalse(config.isPrintProperties());
        Assert.assertEquals(StandardCharsets.UTF_8, config.getCharset());
        Assert.assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, config.getConsumeFromWhere());
        Assert.assertNull(config.getConsumeTimestamp());
    }

    @Test
    public void testCompleteConfiguration() throws Exception {
        PushConsumeMessageConfig config = parse(
            "-t", "TopicA",
            "-g", "GroupA",
            "-s", "TagA || TagB",
            "-i", "diagnostic-instance",
            "-c", "23",
            "-w", "17",
            "-b", "32",
            "-o", "true",
            "-m", "true",
            "-r", "TraceTopic",
            "-d", "false",
            "-p", "true",
            "-a", "UTF-16",
            "-f", "timestamp",
            "-x", "20260718120000");

        Assert.assertEquals("TagA || TagB", config.getSubExpression());
        Assert.assertEquals("diagnostic-instance", config.getInstanceName());
        Assert.assertEquals(23, config.getMaxMessages());
        Assert.assertEquals(17_000, config.getMaxWaitMillis());
        Assert.assertEquals(32, config.getBatchSize());
        Assert.assertTrue(config.isOrderly());
        Assert.assertTrue(config.isMessageTraceEnabled());
        Assert.assertEquals("TraceTopic", config.getCustomizedTraceTopic());
        Assert.assertFalse(config.isPrintBody());
        Assert.assertTrue(config.isPrintProperties());
        Assert.assertEquals("UTF-16", config.getCharset().name());
        Assert.assertEquals(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP, config.getConsumeFromWhere());
        Assert.assertEquals("20260718120000", config.getConsumeTimestamp());
    }

    @Test
    public void testFirstOffsetConfiguration() throws Exception {
        PushConsumeMessageConfig config = parse("-t", "TopicA", "-g", "GroupA", "-f", "FiRsT");
        Assert.assertEquals(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, config.getConsumeFromWhere());
    }

    @Test
    public void testTrimValues() throws Exception {
        PushConsumeMessageConfig config = parse("-t", " TopicA ", "-g", " GroupA ", "-s", " TagA ");
        Assert.assertEquals("TopicA", config.getTopic());
        Assert.assertEquals("GroupA", config.getConsumerGroup());
        Assert.assertEquals("TagA", config.getSubExpression());
    }

    @Test
    public void testNegativeMessageCountRejected() {
        assertInvalid("messageCount must be greater", "-t", "TopicA", "-g", "GroupA", "-c", "-1");
    }

    @Test
    public void testInvalidMessageCountRejected() {
        assertInvalid("messageCount must be an integer", "-t", "TopicA", "-g", "GroupA", "-c", "many");
    }

    @Test
    public void testNegativeWaitRejected() {
        assertInvalid("maxWaitSeconds must be greater", "-t", "TopicA", "-g", "GroupA", "-w", "-2");
    }

    @Test
    public void testInvalidWaitRejected() {
        assertInvalid("maxWaitSeconds must be an integer", "-t", "TopicA", "-g", "GroupA", "-w", "later");
    }

    @Test
    public void testBatchSizeBelowRangeRejected() {
        assertInvalid("batchSize must be between", "-t", "TopicA", "-g", "GroupA", "-b", "0");
    }

    @Test
    public void testBatchSizeAboveRangeRejected() {
        assertInvalid("batchSize must be between", "-t", "TopicA", "-g", "GroupA", "-b", "1025");
    }

    @Test
    public void testInvalidBatchSizeRejected() {
        assertInvalid("batchSize must be an integer", "-t", "TopicA", "-g", "GroupA", "-b", "large");
    }

    @Test
    public void testInvalidBooleanRejected() {
        assertInvalid("orderly must be true or false", "-t", "TopicA", "-g", "GroupA", "-o", "yes");
    }

    @Test
    public void testUnsupportedCharsetRejected() {
        assertInvalid("Unsupported charset", "-t", "TopicA", "-g", "GroupA", "-a", "not-a-charset");
    }

    @Test
    public void testUnknownConsumeFromWhereRejected() {
        assertInvalid("consumeFromWhere must be", "-t", "TopicA", "-g", "GroupA", "-f", "middle");
    }

    @Test
    public void testTimestampRequiredForTimestampMode() {
        assertInvalid("consumeTimestamp is required", "-t", "TopicA", "-g", "GroupA", "-f", "timestamp");
    }

    @Test
    public void testTimestampRejectedForOtherModes() {
        assertInvalid("consumeTimestamp can only", "-t", "TopicA", "-g", "GroupA", "-x", "20260718120000");
    }

    @Test
    public void testInvalidTimestampFormatRejected() {
        assertInvalid("consumeTimestamp must use", "-t", "TopicA", "-g", "GroupA", "-f", "timestamp",
            "-x", "2026-07-18");
    }

    @Test
    public void testWaitOverflowRejected() {
        assertInvalid("maxWaitSeconds is too large", "-t", "TopicA", "-g", "GroupA", "-w",
            Long.toString(Long.MAX_VALUE));
    }

    @Test
    public void testTraceTopicRequiresTraceEnabled() {
        assertInvalid("customizedTraceTopic requires", "-t", "TopicA", "-g", "GroupA", "-r", "TraceTopic");
    }

    @Test
    public void testBlankTopicRejected() {
        assertInvalid("topic must not be blank", "-t", "   ", "-g", "GroupA");
    }

    @Test
    public void testBlankConsumerGroupRejected() {
        assertInvalid("consumerGroup must not be blank", "-t", "TopicA", "-g", "   ");
    }

    private PushConsumeMessageConfig parse(String... arguments) throws Exception {
        Options options = new PushConsumeMessageCommand().buildCommandlineOptions(new Options());
        CommandLine commandLine = new DefaultParser().parse(options, arguments);
        return PushConsumeMessageConfig.fromCommandLine(commandLine);
    }

    private void assertInvalid(String expectedMessage, String... arguments) {
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> parse(arguments));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(expectedMessage));
    }
}
