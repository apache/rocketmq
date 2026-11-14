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

package org.apache.rocketmq.broker.subscription;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;

public class ForbiddenTest {
    @Test
    public void testBrokerRestart() throws Exception {
        SubscriptionGroupManager s = new SubscriptionGroupManager(
            new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig()));
        s.updateForbidden("g", "t", 0, true);
        assertEquals(1, s.getForbidden("g", "t"));
        assertEquals(true, s.getForbidden("g", "t", 0));

        s.updateForbidden("g", "t", 1, true);
        assertEquals(3, s.getForbidden("g", "t"));
        assertEquals(true, s.getForbidden("g", "t", 1));

        s.updateForbidden("g", "t", 2, true);
        assertEquals(7, s.getForbidden("g", "t"));
        assertEquals(true, s.getForbidden("g", "t", 2));

        s.updateForbidden("g", "t", 1, false);
        assertEquals(5, s.getForbidden("g", "t"));
        assertEquals(false, s.getForbidden("g", "t", 1));

        s.updateForbidden("g", "t", 1, false);
        assertEquals(5, s.getForbidden("g", "t"));
        assertEquals(false, s.getForbidden("g", "t", 1));

        s.updateForbidden("g", "t", 0, false);
        assertEquals(4, s.getForbidden("g", "t"));
        assertEquals(false, s.getForbidden("g", "t", 0));

        s.updateForbidden("g", "t", 2, false);
        assertEquals(0, s.getForbidden("g", "t"));
        assertEquals(false, s.getForbidden("g", "t", 2));
    }

    @Test
    public void testClearOneTopicForbiddenKeepsOtherTopicsOfTheSameGroup() throws Exception {
        SubscriptionGroupManager s = new SubscriptionGroupManager(
            new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig()));
        s.updateForbidden("g", "t1", 0, true);
        s.updateForbidden("g", "t2", 0, true);
        assertEquals(true, s.getForbidden("g", "t1", 0));
        assertEquals(true, s.getForbidden("g", "t2", 0));
        long counterBeforeClear = s.getDataVersion().getCounter().get();

        s.updateForbidden("g", "t1", 0, false);

        // Only t1's forbidden bit is cleared; t2 keeps its own state.
        assertEquals(false, s.getForbidden("g", "t1", 0));
        assertEquals(true, s.getForbidden("g", "t2", 0));
        assertEquals(1, s.getForbiddenTable().get("g").size());
        // Clearing a forbidden bit must bump the data version and persist, so
        // brokers that sync or restart do not resurrect the stale bit.
        assertThat(s.getDataVersion().getCounter().get(), greaterThan(counterBeforeClear));

        s.updateForbidden("g", "t2", 0, false);
        assertEquals(0, s.getForbidden("g", "t2"));
        assertNull(s.getForbiddenTable().get("g"));
    }

}
