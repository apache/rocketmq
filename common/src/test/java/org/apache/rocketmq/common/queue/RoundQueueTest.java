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

package org.apache.rocketmq.common.queue;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RoundQueueTest {

    @Test
    public void testDuplicateValuesAreRejected() {
        RoundQueue<String> queue = new RoundQueue<>(2);

        assertTrue(queue.put(new String("first")));
        assertFalse(queue.put(new String("first")));
        assertTrue(queue.put("second"));
        assertFalse(queue.put("first"));
        assertFalse(queue.put("second"));
    }

    @Test
    public void testCapacityOneEvictsPreviousValue() {
        RoundQueue<String> queue = new RoundQueue<>(1);

        assertTrue(queue.put("first"));
        assertFalse(queue.put("first"));
        assertTrue(queue.put("second"));
        assertFalse(queue.put("second"));
        assertTrue(queue.put("first"));
    }

    @Test
    public void testFullQueueEvictsOldestValue() {
        RoundQueue<String> queue = new RoundQueue<>(3);
        assertTrue(queue.put("first"));
        assertTrue(queue.put("second"));
        assertTrue(queue.put("third"));

        assertTrue(queue.put("fourth"));

        assertFalse(queue.put("second"));
        assertFalse(queue.put("third"));
        assertFalse(queue.put("fourth"));
        assertTrue(queue.put("first"));
    }

    @Test
    public void testDuplicateDoesNotRefreshInsertionOrder() {
        RoundQueue<String> queue = new RoundQueue<>(2);
        assertTrue(queue.put("first"));
        assertTrue(queue.put("second"));

        assertFalse(queue.put("first"));
        assertTrue(queue.put("third"));

        assertFalse(queue.put("second"));
        assertFalse(queue.put("third"));
        assertTrue(queue.put("first"));
    }
}
