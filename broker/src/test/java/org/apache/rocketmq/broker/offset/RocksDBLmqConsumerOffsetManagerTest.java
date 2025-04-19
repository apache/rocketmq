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

package org.apache.rocketmq.broker.offset;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.config.v1.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class RocksDBLmqConsumerOffsetManagerTest {
    private static final String LMQ_GROUP = MixAll.LMQ_PREFIX + "FooBarGroup";
    private static final String NON_LMQ_GROUP = "nonLmqGroup";

    private static final String LMQ_TOPIC = MixAll.LMQ_PREFIX + "FooBarTopic";
    private static final String NON_LMQ_TOPIC = "FooBarTopic";
    private static final int QUEUE_ID = 0;
    private static final long OFFSET = 12345;

    private BrokerController brokerController;

    private RocksDBConsumerOffsetManager offsetManager;

    @Before
    public void setUp() {
        brokerController = Mockito.mock(BrokerController.class);
        when(brokerController.getMessageStoreConfig()).thenReturn(Mockito.mock(MessageStoreConfig.class));
        when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        offsetManager = new RocksDBConsumerOffsetManager(brokerController);
    }


    @Test
    public void testQueryOffsetForNonLmq() {
        long actualOffset = offsetManager.queryOffset(NON_LMQ_GROUP, NON_LMQ_TOPIC, QUEUE_ID);
        // Verify
        assertEquals("Offset should not be null.", -1, actualOffset);
    }


    @Test
    public void testQueryOffsetForLmqGroupWithExistingOffset() {
        offsetManager.commitOffset("127.0.0.1",LMQ_GROUP, LMQ_TOPIC, QUEUE_ID, OFFSET);

        // Act
        Map<Integer, Long> actualOffsets = offsetManager.queryOffset(LMQ_GROUP, LMQ_TOPIC);

        // Assert
        assertNotNull(actualOffsets);
        assertEquals(1, actualOffsets.size());
        assertEquals(OFFSET, (long) actualOffsets.get(0));
    }

    @Test
    public void testQueryOffsetForLmqGroupWithoutExistingOffset() {
        // Act
        Map<Integer, Long> actualOffsets = offsetManager.queryOffset(LMQ_GROUP, "nonExistingTopic");
        // Assert
        assertNull(actualOffsets);
    }

    @Test
    public void testQueryOffsetForNonLmqGroup() {
        // Arrange
        Map<Integer, Long> mockOffsets = new HashMap<>();
        mockOffsets.put(QUEUE_ID, OFFSET);

        offsetManager.commitOffset("clientHost", NON_LMQ_GROUP, NON_LMQ_TOPIC, QUEUE_ID, OFFSET);

        // Act
        Map<Integer, Long> actualOffsets = offsetManager.queryOffset(NON_LMQ_GROUP, NON_LMQ_TOPIC);

        // Assert
        assertNotNull(actualOffsets);
        assertEquals("Offsets should match the mocked return value for non-LMQ groups", mockOffsets, actualOffsets);
    }

    @Test
    public void testCommitOffsetForLmq() {
        // Execute
        offsetManager.commitOffset("clientHost", LMQ_GROUP, LMQ_TOPIC, QUEUE_ID, OFFSET);
        // Verify
        Long expectedOffset = offsetManager.getOffsetTable().get(getLMQKey()).get(QUEUE_ID);
        assertEquals("Offset should be updated correctly.", OFFSET, expectedOffset.longValue());
    }

    private String getLMQKey() {
        return LMQ_TOPIC + "@" + LMQ_GROUP;
    }
}
