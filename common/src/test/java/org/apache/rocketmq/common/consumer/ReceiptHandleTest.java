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
package org.apache.rocketmq.common.consumer;

import org.apache.rocketmq.common.KeyBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReceiptHandleTest {

    @Test
    public void testEncodeAndDecode() {
        long startOffset = 1000L;
        long retrieveTime = System.currentTimeMillis();
        long invisibleTime = 1000L;
        int reviveQueueId = 1;
        String topicType = "NORMAL";
        String brokerName = "BrokerA";
        int queueId = 2;
        long offset = 2000L;
        long commitLogOffset = 3000L;
        ReceiptHandle receiptHandle = ReceiptHandle.builder()
            .startOffset(startOffset)
            .retrieveTime(retrieveTime)
            .invisibleTime(invisibleTime)
            .reviveQueueId(reviveQueueId)
            .topicType(topicType)
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(offset)
            .commitLogOffset(commitLogOffset)
            .build();

        String encoded = receiptHandle.encode();
        ReceiptHandle decoded = ReceiptHandle.decode(encoded);

        assertEquals(receiptHandle.getStartOffset(), decoded.getStartOffset());
        assertEquals(receiptHandle.getRetrieveTime(), decoded.getRetrieveTime());
        assertEquals(receiptHandle.getInvisibleTime(), decoded.getInvisibleTime());
        assertEquals(receiptHandle.getReviveQueueId(), decoded.getReviveQueueId());
        assertEquals(receiptHandle.getTopicType(), decoded.getTopicType());
        assertEquals(receiptHandle.getBrokerName(), decoded.getBrokerName());
        assertEquals(receiptHandle.getQueueId(), decoded.getQueueId());
        assertEquals(receiptHandle.getOffset(), decoded.getOffset());
        assertEquals(receiptHandle.getCommitLogOffset(), decoded.getCommitLogOffset());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeWithInvalidString() {
        String invalidReceiptHandle = "invalid_data";

        ReceiptHandle.decode(invalidReceiptHandle);
    }

    @Test
    public void testIsExpired() {
        long startOffset = 1000L;
        long retrieveTime = System.currentTimeMillis();
        long invisibleTime = 1000L;
        int reviveQueueId = 1;
        String topicType = "NORMAL";
        String brokerName = "BrokerA";
        int queueId = 2;
        long offset = 2000L;
        long commitLogOffset = 3000L;
        long pastTime = System.currentTimeMillis() - 1000L;
        ReceiptHandle receiptHandle = new ReceiptHandle(startOffset, retrieveTime, invisibleTime, pastTime, reviveQueueId, topicType, brokerName, queueId, offset, commitLogOffset, "");

        boolean isExpired = receiptHandle.isExpired();

        assertTrue(isExpired);
    }

    @Test
    public void testGetRealTopic() {
        // Arrange
        String topic = "TestTopic";
        String groupName = "TestGroup";
        ReceiptHandle receiptHandle = ReceiptHandle.builder()
            .topicType(ReceiptHandle.RETRY_TOPIC)
            .build();

        String realTopic = receiptHandle.getRealTopic(topic, groupName);

        assertEquals(KeyBuilder.buildPopRetryTopicV1(topic, groupName), realTopic);
    }
}
