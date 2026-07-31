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

package org.apache.rocketmq.proxy.grpc.admin.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ListClientsFilterTest {

    @Test
    public void testDefaultConstructor() {
        ListClientsFilter filter = new ListClientsFilter();
        assertNull(filter.getGroup());
        assertNull(filter.getTopic());
        assertNull(filter.getClientIdPrefix());
        assertNull(filter.getLanguage());
        assertEquals(0, filter.getConnectTimeStart());
        assertEquals(0, filter.getConnectTimeEnd());
    }

    @Test
    public void testSettersAndGetters() {
        ListClientsFilter filter = new ListClientsFilter();
        filter.setGroup("groupA");
        filter.setTopic("topicA");
        filter.setClientIdPrefix("client-");
        filter.setLanguage("JAVA");
        filter.setConnectTimeStart(1000L);
        filter.setConnectTimeEnd(2000L);

        assertEquals("groupA", filter.getGroup());
        assertEquals("topicA", filter.getTopic());
        assertEquals("client-", filter.getClientIdPrefix());
        assertEquals("JAVA", filter.getLanguage());
        assertEquals(1000L, filter.getConnectTimeStart());
        assertEquals(2000L, filter.getConnectTimeEnd());
    }

    @Test
    public void testHasFilterEmpty() {
        ListClientsFilter filter = new ListClientsFilter();
        assertFalse(filter.hasFilter());
    }

    @Test
    public void testHasFilterEachField() {
        ListClientsFilter filter = new ListClientsFilter();
        filter.setGroup("groupA");
        assertTrue(filter.hasFilter());

        filter = new ListClientsFilter();
        filter.setTopic("topicA");
        assertTrue(filter.hasFilter());

        filter = new ListClientsFilter();
        filter.setClientIdPrefix("client-");
        assertTrue(filter.hasFilter());

        filter = new ListClientsFilter();
        filter.setLanguage("JAVA");
        assertTrue(filter.hasFilter());

        filter = new ListClientsFilter();
        filter.setConnectTimeStart(1000L);
        assertTrue(filter.hasFilter());

        filter = new ListClientsFilter();
        filter.setConnectTimeEnd(2000L);
        assertTrue(filter.hasFilter());
    }
}
