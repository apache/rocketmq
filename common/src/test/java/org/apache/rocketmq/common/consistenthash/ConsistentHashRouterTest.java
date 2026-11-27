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

package org.apache.rocketmq.common.consistenthash;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ConsistentHashRouterTest {

    @Test
    public void testRouteAtRingBoundaryAndWrapAround() {
        Node first = () -> "10";
        Node second = () -> "30";
        // The two virtual nodes occupy positions 10 and 30.
        HashFunction hash = key -> Long.parseLong(key.split("-", 2)[0]);
        ConsistentHashRouter<Node> router = new ConsistentHashRouter<>(
            Arrays.asList(first, second), 1, hash);

        assertSame(first, router.routeNode("10"));
        assertSame(second, router.routeNode("11"));
        assertSame(second, router.routeNode("30"));
        assertSame(first, router.routeNode("31"));
    }

    @Test
    public void testRemovingPhysicalNodeRemovesAllItsReplicas() {
        Node removed = () -> "consumer-a";
        Node retained = () -> "consumer-b";
        ConsistentHashRouter<Node> router = new ConsistentHashRouter<>(
            Arrays.asList(removed, retained), 2, String::hashCode);
        router.addNode(removed, 1);

        assertEquals(3, router.getExistingReplicas(removed));
        assertSame(removed, router.routeNode("consumer-a-2"));

        router.removeNode(removed);

        assertEquals(0, router.getExistingReplicas(removed));
        assertEquals(2, router.getExistingReplicas(retained));
        assertSame(retained, router.routeNode("consumer-a-2"));
    }

    @Test
    public void testEmptyRingHasNoRoute() {
        ConsistentHashRouter<Node> router = new ConsistentHashRouter<>(
            Collections.emptyList(), 1);

        assertNull(router.routeNode("message-queue"));
    }

    @Test
    public void testRouteAcrossSignedHashRange() {
        Node first = () -> "first";
        Node last = () -> "last";
        HashFunction hash = key -> {
            if ("first-0".equals(key)) {
                return Long.MIN_VALUE + 1;
            }
            if ("last-0".equals(key)) {
                return Long.MAX_VALUE - 1;
            }
            return Long.parseLong(key);
        };
        ConsistentHashRouter<Node> router = new ConsistentHashRouter<>(Arrays.asList(first, last), 1, hash);

        assertSame(first, router.routeNode(Long.toString(Long.MIN_VALUE)));
        assertSame(first, router.routeNode(Long.toString(Long.MIN_VALUE + 1)));
        assertSame(last, router.routeNode("0"));
        assertSame(last, router.routeNode(Long.toString(Long.MAX_VALUE - 1)));
        assertSame(first, router.routeNode(Long.toString(Long.MAX_VALUE)));
    }

    @Test
    public void testRouteAfterHashCollisionAndRemoval() {
        Node first = () -> "first";
        Node replacement = () -> "replacement";
        ConsistentHashRouter<Node> router = new ConsistentHashRouter<>(
            Collections.singletonList(first), 1, key -> 10L);

        router.addNode(replacement, 1);
        assertSame(replacement, router.routeNode("queue"));
        router.removeNode(first);
        assertSame(replacement, router.routeNode("queue"));
        router.removeNode(replacement);
        assertNull(router.routeNode("queue"));
    }
}
