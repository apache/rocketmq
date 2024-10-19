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
package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AllocateMessageQueueByGrayTest {

    @InjectMocks
    private AllocateMessageQueueByGray allocateMessageQueueByGray;

    private List<MessageQueue> mqAll;
    private List<String> cidAll;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        mqAll = new ArrayList<>();
        cidAll = new ArrayList<>();
    }

    @Test
    public void testAllocate_NoGrayReleaseClient_ShouldAllocateByAverageStrategy() {
        cidAll.addAll(Arrays.asList("CID1", "CID2", "CID3"));
        mqAll.addAll(Arrays.asList(
                new MessageQueue("Topic1", "Broker1", 0),
                new MessageQueue("Topic1", "Broker1", 1),
                new MessageQueue("Topic1", "Broker1", 2),
                new MessageQueue("Topic1", "Broker1", 3),
                new MessageQueue("Topic1", "Broker2", 0),
                new MessageQueue("Topic1", "Broker2", 1),
                new MessageQueue("Topic1", "Broker2", 2),
                new MessageQueue("Topic1", "Broker2", 3)
        ));

        List<MessageQueue> result = allocateMessageQueueByGray.allocate("CG", "CID1", mqAll, cidAll);

        assertEquals(3, result.size());
        assertTrue(result.contains(new MessageQueue("Topic1", "Broker1", 0)));
        assertTrue(result.contains(new MessageQueue("Topic1", "Broker1", 1)) || result.contains(new MessageQueue("Topic1", "Broker2", 0)));
    }

    @Test
    public void testAllocate_HasGrayReleaseClient_ShouldAllocateByGrayReleaseStrategy() {
        cidAll.addAll(Arrays.asList("CID1@gray[0.5]", "CID2@gray[0.5]", "CID3"));
        mqAll.addAll(Arrays.asList(
                new MessageQueue("Topic1", "Broker1", 0),
                new MessageQueue("Topic1", "Broker1", 1),
                new MessageQueue("Topic1", "Broker1", 2),
                new MessageQueue("Topic1", "Broker1", 3),
                new MessageQueue("Topic1", "Broker2", 0),
                new MessageQueue("Topic1", "Broker2", 1),
                new MessageQueue("Topic1", "Broker2", 2),
                new MessageQueue("Topic1", "Broker2", 3)
        ));

        List<MessageQueue> result = allocateMessageQueueByGray.allocate("CG", "CID1@gray[0.5]", mqAll, cidAll);

        assertEquals(2, result.size());
        assertTrue(result.contains(new MessageQueue("Topic1", "Broker1", 2)));
    }

    @Test
    public void testAllocate4Pop_HasGrayReleaseClient_ShouldAllocateGrayMessageQueues() {
        cidAll.addAll(Arrays.asList("CID1", "CID2@gray[0.5]", "CID3"));
        mqAll.addAll(Arrays.asList(
                new MessageQueue("Topic1", "Broker1", 0),
                new MessageQueue("Topic1", "Broker1", 1),
                new MessageQueue("Topic1", "Broker1", 2),
                new MessageQueue("Topic1", "Broker1", 3),
                new MessageQueue("Topic1", "Broker2", 0),
                new MessageQueue("Topic1", "Broker2", 1),
                new MessageQueue("Topic1", "Broker2", 2),
                new MessageQueue("Topic1", "Broker2", 3)
        ));

        List<MessageQueue> result = allocateMessageQueueByGray.allocate4Pop("CG", "CID1", mqAll, cidAll, 0);

        assertEquals(4, result.size());
        assertTrue(result.contains(new MessageQueue("Topic1", "Broker1", 0)));
        assertTrue(result.contains(new MessageQueue("Topic1", "Broker2", 1)));
    }


    @Test
    public void testAllocate4Pop_HasGrayReleaseClient_ShouldAllocateGrayMessageQueues2() {
        cidAll.addAll(Arrays.asList("CID1", "CID2@gray[0.2]", "CID3@gray[0.2]", "CID4", "CID5"));
        mqAll.addAll(Arrays.asList(
                new MessageQueue("Topic1", "Broker1", 0),
                new MessageQueue("Topic1", "Broker1", 1),
                new MessageQueue("Topic1", "Broker1", 2),
                new MessageQueue("Topic1", "Broker1", 3),
                new MessageQueue("Topic1", "Broker2", 0),
                new MessageQueue("Topic1", "Broker2", 1),
                new MessageQueue("Topic1", "Broker2", 2),
                new MessageQueue("Topic1", "Broker2", 3)
        ));

        List<MessageQueue> result = allocateMessageQueueByGray.allocate4Pop("CG", "CID1", mqAll, cidAll, 0);
        assertEquals(6, result.size());
        assertTrue(result.contains(new MessageQueue("Topic1", "Broker1", 0)));
        assertTrue(result.contains(new MessageQueue("Topic1", "Broker2", 2)));
    }

    @Test
    public void testSplitMessageQueues_EmptySource_ShouldReturnEmptyLists() {
        Pair<List<MessageQueue>, List<MessageQueue>> result = allocateMessageQueueByGray.splitMessageQueues(Collections.emptyList(), 0.5);

        assertTrue(result.getObject1().isEmpty());
        assertTrue(result.getObject2().isEmpty());
    }

    @Test
    public void testSplitMessageQueues_ValidGrayQueueRatio_ShouldSplitCorrectly() {
        mqAll.addAll(Arrays.asList(
                new MessageQueue("Topic1", "Broker1", 0),
                new MessageQueue("Topic1", "Broker1", 1),
                new MessageQueue("Topic1", "Broker1", 2),
                new MessageQueue("Topic1", "Broker2", 0),
                new MessageQueue("Topic1", "Broker2", 1),
                new MessageQueue("Topic1", "Broker2", 2)
        ));

        Pair<List<MessageQueue>, List<MessageQueue>> result = allocateMessageQueueByGray.splitMessageQueues(mqAll, 0.2);

        assertEquals(2, result.getObject1().size());
        assertEquals(4, result.getObject2().size());
    }

    @Test
    public void testGetGrayQueueRatio_ValidGrayTag_ShouldReturnCorrectRatio() {
        double result = allocateMessageQueueByGray.getGrayQueueRatio(Arrays.asList("CID1@gray[0.5]", "CID2"));
        assertEquals(0.5, result, 0.0001);
    }

    @Test
    public void testGetGrayQueueRatio_InvalidGrayTag_ShouldReturnDefaultRatio() {
        double result = allocateMessageQueueByGray.getGrayQueueRatio(Arrays.asList("CID1", "CID2"));
        assertEquals(0.1, result, 0.0001);
    }

    @Test
    public void testHasGrayTag_ValidGrayTag_ShouldReturnTrue() {
        boolean result = AllocateMessageQueueByGray.hasGrayTag(Collections.singletonList("CID1@gray[0.5]"));
        assertTrue(result);
    }

    @Test
    public void testHasGrayTag_InvalidGrayTag_ShouldReturnFalse() {
        boolean result = AllocateMessageQueueByGray.hasGrayTag(Collections.singletonList("CID1"));
        assertFalse(result);
    }
}
