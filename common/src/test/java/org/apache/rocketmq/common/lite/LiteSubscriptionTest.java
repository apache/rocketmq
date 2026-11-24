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

package org.apache.rocketmq.common.lite;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LiteSubscriptionTest {

    private LiteSubscription subscription;

    @Before
    public void setUp() {
        subscription = new LiteSubscription();
    }

    // ========== Group 1: chainable setters ==========

    @Test
    public void setGroup_returnsThis() {
        LiteSubscription result = subscription.setGroup("testGroup");
        assertSame(subscription, result);
        assertEquals("testGroup", subscription.getGroup());
    }

    @Test
    public void setTopic_returnsThis() {
        LiteSubscription result = subscription.setTopic("testTopic");
        assertSame(subscription, result);
        assertEquals("testTopic", subscription.getTopic());
    }

    @Test
    public void setLmqSet_returnsThis() {
        Set<String> newSet = new HashSet<>();
        newSet.add("lmq1");
        LiteSubscription result = subscription.setLmqSet(newSet);
        assertSame(subscription, result);
        assertTrue(subscription.getLmqSet().contains("lmq1"));
    }

    // ========== Group 2: touch ==========

    @Test
    public void touch_updatesTimeAndReturnsThis() throws InterruptedException {
        long before = subscription.getUpdateTime();
        Thread.sleep(10);
        LiteSubscription result = subscription.touch();
        assertSame(subscription, result);
        assertTrue(subscription.getUpdateTime() > before);
    }

    // ========== Group 3: lmqSet add/remove ==========

    @Test
    public void addLmq_newElement_returnsTrue() {
        assertTrue(subscription.addLmq("lmq1"));
        assertEquals(1, subscription.getLmqSet().size());
    }

    @Test
    public void addLmq_duplicate_returnsFalse() {
        subscription.addLmq("lmq1");
        assertFalse(subscription.addLmq("lmq1"));
        assertEquals(1, subscription.getLmqSet().size());
    }

    @Test
    public void removeLmq_existing_returnsTrue() {
        subscription.addLmq("lmq1");
        assertTrue(subscription.removeLmq("lmq1"));
        assertTrue(subscription.getLmqSet().isEmpty());
    }

    @Test
    public void removeLmq_absent_returnsFalse() {
        assertFalse(subscription.removeLmq("nonexistent"));
    }

    // ========== Group 4: setLmqSet replacement semantics ==========

    @Test
    public void setLmqSet_clearsOldAndAddsNew() {
        subscription.addLmq("old1");
        subscription.addLmq("old2");

        Set<String> newSet = new HashSet<>();
        newSet.add("new1");
        subscription.setLmqSet(newSet);

        assertEquals(1, subscription.getLmqSet().size());
        assertTrue(subscription.getLmqSet().contains("new1"));
        assertFalse(subscription.getLmqSet().contains("old1"));
    }

    // ========== Group 5: removals static utility ==========

    @Test
    public void removals_normalDiff() {
        Set<String> current = new HashSet<>();
        current.add("a");
        current.add("b");
        current.add("c");
        Set<String> target = new HashSet<>();
        target.add("b");

        Set<String> result = LiteSubscription.removals(current, target);
        assertEquals(2, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("c"));
    }

    @Test
    public void removals_targetNull() {
        Set<String> current = new HashSet<>();
        current.add("a");
        current.add("b");

        Set<String> result = LiteSubscription.removals(current, null);
        assertEquals(2, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
    }

    @Test
    public void removals_noDiff() {
        Set<String> current = new HashSet<>();
        current.add("a");
        Set<String> target = new HashSet<>();
        target.add("a");
        target.add("b");

        Set<String> result = LiteSubscription.removals(current, target);
        assertTrue(result.isEmpty());
    }

    @Test
    public void removals_emptyCurrent() {
        Set<String> result = LiteSubscription.removals(Collections.emptySet(), Collections.singleton("a"));
        assertTrue(result.isEmpty());
    }

    // ========== Group 6: thread safety type check ==========

    @Test
    public void lmqSet_isConcurrentSafe() {
        assertTrue(subscription.getLmqSet().getClass().getName().contains("ConcurrentHashMap"));
    }
}
