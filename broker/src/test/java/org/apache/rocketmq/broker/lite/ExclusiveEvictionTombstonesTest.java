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

package org.apache.rocketmq.broker.lite;

import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExclusiveEvictionTombstonesTest {

    private ExclusiveEvictionTombstones tombstones;

    @Before
    public void setUp() {
        tombstones = new ExclusiveEvictionTombstones();
    }

    @Test
    public void testAddAndContains() {
        // empty store
        assertFalse(tombstones.contains("client1", "lmq1"));
        assertEquals(0, tombstones.size());

        // basic add
        tombstones.add("client1", "lmq1");
        assertTrue(tombstones.contains("client1", "lmq1"));
        assertFalse(tombstones.contains("client1", "lmq2")); // same client, different lmq
        assertFalse(tombstones.contains("client2", "lmq1")); // different client, same lmq
        assertEquals(1, tombstones.size());

        // duplicate add is idempotent
        tombstones.add("client1", "lmq1");
        assertEquals(1, tombstones.size());
    }

    @Test
    public void testRemoveAllOf() {
        tombstones.add("client1", "lmq1");
        tombstones.add("client1", "lmq2");
        tombstones.add("client2", "lmq1");

        // non-existent client is no-op
        tombstones.removeAllOf("client3");
        assertEquals(3, tombstones.size());

        // removes only target client
        tombstones.removeAllOf("client1");
        assertFalse(tombstones.contains("client1", "lmq1"));
        assertFalse(tombstones.contains("client1", "lmq2"));
        assertTrue(tombstones.contains("client2", "lmq1"));
        assertEquals(1, tombstones.size());
    }

    @Test
    public void testRemoveStale() {
        tombstones.add("client1", "lmq1");
        tombstones.add("client1", "lmq2");
        tombstones.add("client1", "lmq3");
        tombstones.add("client2", "lmq1");

        // removes stale, keeps active, does not affect other clients
        Set<String> activeSet = new HashSet<>();
        activeSet.add("lmq2");
        activeSet.add("lmq3");
        tombstones.removeStale("client1", activeSet);

        assertFalse(tombstones.contains("client1", "lmq1")); // removed (not in activeSet)
        assertTrue(tombstones.contains("client1", "lmq2"));  // retained
        assertTrue(tombstones.contains("client1", "lmq3"));  // retained
        assertTrue(tombstones.contains("client2", "lmq1"));  // unaffected
        assertEquals(3, tombstones.size());

        // empty activeSet clears all for that client
        tombstones.removeStale("client1", new HashSet<>());
        assertFalse(tombstones.contains("client1", "lmq2"));
        assertFalse(tombstones.contains("client1", "lmq3"));
        assertTrue(tombstones.contains("client2", "lmq1")); // still unaffected
    }

    @Test
    public void testSize() {
        assertEquals(0, tombstones.size());

        tombstones.add("c1", "l1");
        assertEquals(1, tombstones.size());

        tombstones.add("c1", "l2");
        assertEquals(2, tombstones.size());

        tombstones.add("c2", "l1");
        assertEquals(3, tombstones.size());

        tombstones.removeAllOf("c1");
        assertEquals(1, tombstones.size());
    }

    /**
     * Verifies that real RocketMQ clientId formats (containing '@') and lmqName formats
     * (e.g. topic@group for wildcard) do not cause cross-client collisions.
     * This is the core safety guarantee of using '\0' separator instead of '@'.
     */
    @Test
    public void testRealClientIdFormat_NoCrossClientCollision() {
        // RocketMQ clientId: IP@instanceName vs IP@instanceName@unitName
        // clientA is a strict prefix of clientB if '@' were the separator
        String clientA = "10.0.0.1@DEFAULT";
        String clientB = "10.0.0.1@DEFAULT@unit1";
        String lmqPlain = "lmq1";
        String lmqWildcard = "parentTopic@wildcardGroup"; // lmqName also contains '@'

        tombstones.add(clientA, lmqPlain);
        tombstones.add(clientA, lmqWildcard);
        tombstones.add(clientB, lmqPlain);

        // basic isolation
        assertTrue(tombstones.contains(clientA, lmqPlain));
        assertTrue(tombstones.contains(clientA, lmqWildcard));
        assertTrue(tombstones.contains(clientB, lmqPlain));
        assertFalse(tombstones.contains(clientA, "parentTopic")); // partial lmqName no match
        assertFalse(tombstones.contains("10.0.0.1@DEFAULT", lmqPlain + "@extra")); // no false match
        assertEquals(3, tombstones.size());

        // removeStale for clientA does NOT affect clientB
        Set<String> activeSet = new HashSet<>();
        activeSet.add(lmqWildcard);
        tombstones.removeStale(clientA, activeSet);

        assertFalse(tombstones.contains(clientA, lmqPlain)); // removed
        assertTrue(tombstones.contains(clientA, lmqWildcard)); // retained
        assertTrue(tombstones.contains(clientB, lmqPlain)); // unaffected
        assertEquals(2, tombstones.size());

        // removeAllOf clientA does NOT affect clientB
        tombstones.removeAllOf(clientA);
        assertFalse(tombstones.contains(clientA, lmqWildcard));
        assertTrue(tombstones.contains(clientB, lmqPlain));
        assertEquals(1, tombstones.size());
    }
}
