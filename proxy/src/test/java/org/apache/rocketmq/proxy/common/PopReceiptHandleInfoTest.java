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

package org.apache.rocketmq.proxy.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PopReceiptHandleInfoTest {

    @Test
    public void testConstructorAndGetters() {
        PopReceiptHandleInfo info = new PopReceiptHandleInfo(
            "groupA", "topicA", 2, "msg-1",
            100L, 1, 3, 1,
            1000L, "handle-1", 2000L,
            60000L, "broker-a", false);

        assertEquals("groupA", info.getGroup());
        assertEquals("topicA", info.getTopic());
        assertEquals(2, info.getQueueId());
        assertEquals("msg-1", info.getMessageId());
        assertEquals(100L, info.getQueueOffset());
        assertEquals(1, info.getReconsumeTimes());
        assertEquals(3, info.getRenewTimes());
        assertEquals(1, info.getRenewRetryTimes());
        assertEquals(1000L, info.getConsumeTimestamp());
        assertEquals("handle-1", info.getReceiptHandle());
        assertEquals(2000L, info.getNextVisibleTime());
        assertEquals(60000L, info.getInvisibleTime());
        assertEquals("broker-a", info.getBrokerName());
        assertFalse(info.isExpired());
    }

    @Test
    public void testExpiredHandle() {
        PopReceiptHandleInfo info = new PopReceiptHandleInfo(
            "groupA", "topicA", 0, "msg-2",
            0L, 0, 0, 0,
            0L, "handle-2", 0L,
            0L, "broker-a", true);

        assertTrue(info.isExpired());
    }
}
