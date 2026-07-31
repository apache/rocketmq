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

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClientInstanceInfoTest {

    @Test
    public void testDefaultConstructor() {
        ClientInstanceInfo info = new ClientInstanceInfo();
        assertNull(info.getClientId());
        assertNull(info.getLanguage());
        assertNull(info.getClientVersion());
        assertNull(info.getProtocol());
        assertNull(info.getAccessPoint());
        assertEquals(0, info.getConnectAt());
        assertEquals(0, info.getLastActiveAt());
        assertNull(info.getRole());
        assertNull(info.getGroup());
        assertNull(info.getTopics());
    }

    @Test
    public void testSettersAndGetters() {
        ClientInstanceInfo info = new ClientInstanceInfo();
        List<String> topics = Arrays.asList("topicA", "topicB");

        info.setClientId("client-1");
        info.setLanguage("JAVA");
        info.setClientVersion("5.0.0");
        info.setProtocol("grpc");
        info.setAccessPoint("127.0.0.1:8081");
        info.setConnectAt(1000L);
        info.setLastActiveAt(2000L);
        info.setRole("CONSUMER");
        info.setGroup("groupA");
        info.setTopics(topics);

        assertEquals("client-1", info.getClientId());
        assertEquals("JAVA", info.getLanguage());
        assertEquals("5.0.0", info.getClientVersion());
        assertEquals("grpc", info.getProtocol());
        assertEquals("127.0.0.1:8081", info.getAccessPoint());
        assertEquals(1000L, info.getConnectAt());
        assertEquals(2000L, info.getLastActiveAt());
        assertEquals("CONSUMER", info.getRole());
        assertEquals("groupA", info.getGroup());
        assertEquals(topics, info.getTopics());
    }

    @Test
    public void testToString() {
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId("client-1");
        info.setLanguage("JAVA");
        info.setGroup("groupA");

        String str = info.toString();
        assertTrue(str.contains("client-1"));
        assertTrue(str.contains("JAVA"));
        assertTrue(str.contains("groupA"));
    }
}
