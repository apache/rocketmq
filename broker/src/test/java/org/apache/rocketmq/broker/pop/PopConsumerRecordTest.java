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

import java.util.UUID;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;

public class PopConsumerRecordTest {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    @Test
    public void retryCodeTest() {
        Assert.assertEquals("NORMAL_TOPIC code should be 0",
            0, PopConsumerRecord.RetryType.NORMAL_TOPIC.getCode());
        Assert.assertEquals("RETRY_TOPIC code should be 1",
            1, PopConsumerRecord.RetryType.RETRY_TOPIC_V1.getCode());
        Assert.assertEquals("RETRY_TOPIC_V2 code should be 2",
            2, PopConsumerRecord.RetryType.RETRY_TOPIC_V2.getCode());
    }

    @Test
    public void deliveryRecordSerializeTest() {
        PopConsumerRecord consumerRecord = new PopConsumerRecord();
        consumerRecord.setPopTime(System.currentTimeMillis());
        consumerRecord.setGroupId("GroupId");
        consumerRecord.setTopicId("TopicId");
        consumerRecord.setQueueId(3);
        consumerRecord.setRetryFlag(PopConsumerRecord.RetryType.RETRY_TOPIC_V1.getCode());
        consumerRecord.setInvisibleTime(20);
        consumerRecord.setOffset(100);
        consumerRecord.setAttemptTimes(2);
        consumerRecord.setAttemptId(UUID.randomUUID().toString().toUpperCase());

        Assert.assertTrue(consumerRecord.isRetry());
        Assert.assertEquals(consumerRecord.getPopTime() + consumerRecord.getInvisibleTime(),
            consumerRecord.getVisibilityTimeout());
        Assert.assertEquals(8 + "GroupId".length() + 1 + "TopicId".length() + 1 + 4 + 1 + 8,
            consumerRecord.getKeyBytes().length);
        log.info("ConsumerRecord={}", consumerRecord.toString());

        PopConsumerRecord decodeRecord = PopConsumerRecord.decode(consumerRecord.getValueBytes());
        PopConsumerRecord consumerRecord2 = new PopConsumerRecord(consumerRecord.getPopTime(),
            consumerRecord.getGroupId(), consumerRecord.getTopicId(), consumerRecord.getQueueId(),
            consumerRecord.getRetryFlag(), consumerRecord.getInvisibleTime(),
            consumerRecord.getOffset(), consumerRecord.getAttemptId());
        Assert.assertEquals(decodeRecord.getPopTime(), consumerRecord2.getPopTime());
        Assert.assertEquals(decodeRecord.getGroupId(), consumerRecord2.getGroupId());
        Assert.assertEquals(decodeRecord.getTopicId(), consumerRecord2.getTopicId());
        Assert.assertEquals(decodeRecord.getQueueId(), consumerRecord2.getQueueId());
        Assert.assertEquals(decodeRecord.getRetryFlag(), consumerRecord2.getRetryFlag());
        Assert.assertEquals(decodeRecord.getInvisibleTime(), consumerRecord2.getInvisibleTime());
        Assert.assertEquals(decodeRecord.getOffset(), consumerRecord2.getOffset());
        Assert.assertEquals(0, consumerRecord2.getAttemptTimes());
        Assert.assertEquals(decodeRecord.getAttemptId(), consumerRecord2.getAttemptId());
    }

    @Test
    public void testSuspendFlagInitialization() {
        // Test constructor without suspend flag (should default to false)
        PopConsumerRecord record1 = new PopConsumerRecord(
            System.currentTimeMillis(), "test-group", "test-topic", 0, 0, 30000L, 100L, "attempt-id");
        Assert.assertFalse("Suspend flag should default to false", record1.isSuspend());

        // Test constructor with suspend flag set to true
        PopConsumerRecord record2 = new PopConsumerRecord(
            System.currentTimeMillis(), "test-group", "test-topic", 0, 0, 30000L, 100L, "attempt-id", true);
        Assert.assertTrue("Suspend flag should be true", record2.isSuspend());

        // Test constructor with suspend flag set to false
        PopConsumerRecord record3 = new PopConsumerRecord(
            System.currentTimeMillis(), "test-group", "test-topic", 0, 0, 30000L, 100L, "attempt-id", false);
        Assert.assertFalse("Suspend flag should be false", record3.isSuspend());
    }

    @Test
    public void testSuspendFlagSerialization() {
        // Test serialization/deserialization with suspend flag
        PopConsumerRecord originalRecord = new PopConsumerRecord(
            1234567890L, "test-group", "test-topic", 0, 0, 30000L, 100L, "attempt-id", true);

        byte[] serialized = originalRecord.getValueBytes();
        PopConsumerRecord deserialized = PopConsumerRecord.decode(serialized);

        Assert.assertTrue("Deserialized record should have suspend flag true", deserialized.isSuspend());
        Assert.assertEquals("Other fields should match", originalRecord.getGroupId(), deserialized.getGroupId());
        Assert.assertEquals("Other fields should match", originalRecord.getTopicId(), deserialized.getTopicId());
        Assert.assertEquals("Other fields should match", originalRecord.getOffset(), deserialized.getOffset());
    }

    @Test
    public void testSuspendFlagGetterSetter() {
        PopConsumerRecord record = new PopConsumerRecord();

        // Test initial value
        Assert.assertFalse("Initial suspend value should be false", record.isSuspend());

        // Test setter
        record.setSuspend(true);
        Assert.assertTrue("After setting to true, should be true", record.isSuspend());

        record.setSuspend(false);
        Assert.assertFalse("After setting to false, should be false", record.isSuspend());
    }

    @Test
    public void testSuspendInToString() {
        PopConsumerRecord record = new PopConsumerRecord(
            1234567890L, "test-group", "test-topic", 0, 0, 30000L, 100L, "attempt-id", true);

        String toString = record.toString();
        Assert.assertTrue("toString should include suspend information", toString.contains("suspend=true"));

        PopConsumerRecord record2 = new PopConsumerRecord(
            1234567890L, "test-group", "test-topic", 0, 0, 30000L, 100L, "attempt-id", false);

        String toString2 = record2.toString();
        Assert.assertTrue("toString should include suspend information", toString2.contains("suspend=false"));
    }

    @Test
    public void testSuspendFlagSerializationWithFalse() {
        // Test serialization/deserialization with suspend flag set to false
        PopConsumerRecord originalRecord = new PopConsumerRecord(
            1234567890L, "test-group", "test-topic", 0, 0, 30000L, 100L, "attempt-id", false);

        byte[] serialized = originalRecord.getValueBytes();
        PopConsumerRecord deserialized = PopConsumerRecord.decode(serialized);

        Assert.assertFalse("Deserialized record should have suspend flag false", deserialized.isSuspend());
        Assert.assertEquals("GroupId should match", originalRecord.getGroupId(), deserialized.getGroupId());
        Assert.assertEquals("TopicId should match", originalRecord.getTopicId(), deserialized.getTopicId());
        Assert.assertEquals("Offset should match", originalRecord.getOffset(), deserialized.getOffset());
        Assert.assertEquals("PopTime should match", originalRecord.getPopTime(), deserialized.getPopTime());
        Assert.assertEquals("QueueId should match", originalRecord.getQueueId(), deserialized.getQueueId());
        Assert.assertEquals("InvisibleTime should match", originalRecord.getInvisibleTime(), deserialized.getInvisibleTime());
        Assert.assertEquals("RetryFlag should match", originalRecord.getRetryFlag(), deserialized.getRetryFlag());
        Assert.assertEquals("AttemptId should match", originalRecord.getAttemptId(), deserialized.getAttemptId());
    }

    @Test
    public void testSuspendFlagJSONSerializationCompleteness() {
        // Test complete serialization/deserialization with all fields including suspend
        long popTime = System.currentTimeMillis();
        String groupId = "test-group";
        String topicId = "test-topic";
        int queueId = 1;
        int retryFlag = PopConsumerRecord.RetryType.RETRY_TOPIC_V2.getCode();
        long invisibleTime = 30000L;
        long offset = 100L;
        String attemptId = UUID.randomUUID().toString().toUpperCase();

        // Test with suspend = true
        PopConsumerRecord recordWithSuspend = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, retryFlag, invisibleTime, offset, attemptId, true);
        recordWithSuspend.setAttemptTimes(3);

        byte[] serialized = recordWithSuspend.getValueBytes();
        PopConsumerRecord deserialized = PopConsumerRecord.decode(serialized);

        Assert.assertTrue("Suspend flag should be true", deserialized.isSuspend());
        Assert.assertEquals("PopTime should match", popTime, deserialized.getPopTime());
        Assert.assertEquals("GroupId should match", groupId, deserialized.getGroupId());
        Assert.assertEquals("TopicId should match", topicId, deserialized.getTopicId());
        Assert.assertEquals("QueueId should match", queueId, deserialized.getQueueId());
        Assert.assertEquals("RetryFlag should match", retryFlag, deserialized.getRetryFlag());
        Assert.assertEquals("InvisibleTime should match", invisibleTime, deserialized.getInvisibleTime());
        Assert.assertEquals("Offset should match", offset, deserialized.getOffset());
        Assert.assertEquals("AttemptTimes should match", 3, deserialized.getAttemptTimes());
        Assert.assertEquals("AttemptId should match", attemptId, deserialized.getAttemptId());

        // Test with suspend = false
        PopConsumerRecord recordWithoutSuspend = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, retryFlag, invisibleTime, offset, attemptId, false);
        recordWithoutSuspend.setAttemptTimes(3);

        serialized = recordWithoutSuspend.getValueBytes();
        deserialized = PopConsumerRecord.decode(serialized);

        Assert.assertFalse("Suspend flag should be false", deserialized.isSuspend());
        Assert.assertEquals("PopTime should match", popTime, deserialized.getPopTime());
        Assert.assertEquals("GroupId should match", groupId, deserialized.getGroupId());
        Assert.assertEquals("TopicId should match", topicId, deserialized.getTopicId());
        Assert.assertEquals("QueueId should match", queueId, deserialized.getQueueId());
        Assert.assertEquals("RetryFlag should match", retryFlag, deserialized.getRetryFlag());
        Assert.assertEquals("InvisibleTime should match", invisibleTime, deserialized.getInvisibleTime());
        Assert.assertEquals("Offset should match", offset, deserialized.getOffset());
        Assert.assertEquals("AttemptTimes should match", 3, deserialized.getAttemptTimes());
        Assert.assertEquals("AttemptId should match", attemptId, deserialized.getAttemptId());
    }

    @Test
    public void testSuspendFlagDefaultValueInNoArgConstructor() {
        // Test that no-arg constructor defaults suspend to false
        PopConsumerRecord record = new PopConsumerRecord();
        Assert.assertFalse("No-arg constructor should default suspend to false", record.isSuspend());

        // Set all fields manually
        record.setPopTime(System.currentTimeMillis());
        record.setGroupId("test-group");
        record.setTopicId("test-topic");
        record.setQueueId(0);
        record.setRetryFlag(0);
        record.setInvisibleTime(30000L);
        record.setOffset(100L);
        record.setAttemptId("attempt-id");
        record.setSuspend(true);

        Assert.assertTrue("After setting suspend to true, should be true", record.isSuspend());

        // Serialize and deserialize to verify
        byte[] serialized = record.getValueBytes();
        PopConsumerRecord deserialized = PopConsumerRecord.decode(serialized);
        Assert.assertTrue("Deserialized record should preserve suspend=true", deserialized.isSuspend());
    }

    @Test
    public void testSuspendFlagInDeliveryRecordSerializeTest() {
        // Enhance existing deliveryRecordSerializeTest to include suspend flag
        PopConsumerRecord consumerRecord = new PopConsumerRecord();
        consumerRecord.setPopTime(System.currentTimeMillis());
        consumerRecord.setGroupId("GroupId");
        consumerRecord.setTopicId("TopicId");
        consumerRecord.setQueueId(3);
        consumerRecord.setRetryFlag(PopConsumerRecord.RetryType.RETRY_TOPIC_V1.getCode());
        consumerRecord.setInvisibleTime(20);
        consumerRecord.setOffset(100);
        consumerRecord.setAttemptTimes(2);
        consumerRecord.setAttemptId(UUID.randomUUID().toString().toUpperCase());
        consumerRecord.setSuspend(true);

        PopConsumerRecord decodeRecord = PopConsumerRecord.decode(consumerRecord.getValueBytes());
        Assert.assertTrue("Decoded record should preserve suspend flag", decodeRecord.isSuspend());
        Assert.assertEquals("Suspend flag should match", consumerRecord.isSuspend(), decodeRecord.isSuspend());

        // Test with suspend = false
        consumerRecord.setSuspend(false);
        decodeRecord = PopConsumerRecord.decode(consumerRecord.getValueBytes());
        Assert.assertFalse("Decoded record should preserve suspend=false", decodeRecord.isSuspend());
        Assert.assertEquals("Suspend flag should match", consumerRecord.isSuspend(), decodeRecord.isSuspend());
    }
}