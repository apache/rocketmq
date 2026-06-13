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
package org.apache.rocketmq.auth.migration.v1;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class PlainAccessConfigTest {

    @Test
    public void testGettersAndSetters() {
        PlainAccessConfig config = new PlainAccessConfig();
        config.setAccessKey("testAk");
        config.setSecretKey("testSk");
        config.setWhiteRemoteAddress("192.168.1.*");
        config.setAdmin(true);
        config.setDefaultTopicPerm("PUB|SUB");
        config.setDefaultGroupPerm("SUB");

        assertEquals("testAk", config.getAccessKey());
        assertEquals("testSk", config.getSecretKey());
        assertEquals("192.168.1.*", config.getWhiteRemoteAddress());
        assertTrue(config.isAdmin());
        assertEquals("PUB|SUB", config.getDefaultTopicPerm());
        assertEquals("SUB", config.getDefaultGroupPerm());
    }

    @Test
    public void testTopicPermsAndGroupPerms() {
        PlainAccessConfig config = new PlainAccessConfig();
        List<String> topicPerms = Arrays.asList("TestTopic=PUB|SUB", "OrderTopic=PUB");
        List<String> groupPerms = Arrays.asList("TestGroup=SUB");

        config.setTopicPerms(topicPerms);
        config.setGroupPerms(groupPerms);

        assertEquals(2, config.getTopicPerms().size());
        assertEquals("TestTopic=PUB|SUB", config.getTopicPerms().get(0));
        assertEquals(1, config.getGroupPerms().size());
        assertEquals("TestGroup=SUB", config.getGroupPerms().get(0));
    }

    @Test
    public void testEqualsSameObject() {
        PlainAccessConfig config = new PlainAccessConfig();
        config.setAccessKey("ak");
        assertEquals(config, config);
    }

    @Test
    public void testEqualsIdenticalConfigs() {
        PlainAccessConfig c1 = new PlainAccessConfig();
        c1.setAccessKey("ak");
        c1.setSecretKey("sk");
        c1.setAdmin(true);

        PlainAccessConfig c2 = new PlainAccessConfig();
        c2.setAccessKey("ak");
        c2.setSecretKey("sk");
        c2.setAdmin(true);

        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    public void testEqualsDifferentConfigs() {
        PlainAccessConfig c1 = new PlainAccessConfig();
        c1.setAccessKey("ak1");

        PlainAccessConfig c2 = new PlainAccessConfig();
        c2.setAccessKey("ak2");

        assertNotEquals(c1, c2);
    }

    @Test
    public void testEqualsNull() {
        PlainAccessConfig config = new PlainAccessConfig();
        assertNotEquals(null, config);
    }

    @Test
    public void testEqualsDifferentClass() {
        PlainAccessConfig config = new PlainAccessConfig();
        assertNotEquals("not a config", config);
    }

    @Test
    public void testEqualsWithDifferentAdmin() {
        PlainAccessConfig c1 = new PlainAccessConfig();
        c1.setAdmin(true);

        PlainAccessConfig c2 = new PlainAccessConfig();
        c2.setAdmin(false);

        assertNotEquals(c1, c2);
    }

    @Test
    public void testEqualsWithDifferentTopicPerms() {
        PlainAccessConfig c1 = new PlainAccessConfig();
        c1.setAccessKey("ak");
        c1.setTopicPerms(Arrays.asList("A=PUB|SUB"));

        PlainAccessConfig c2 = new PlainAccessConfig();
        c2.setAccessKey("ak");
        c2.setTopicPerms(Arrays.asList("B=PUB|SUB"));

        assertNotEquals(c1, c2);
    }

    @Test
    public void testEqualsWithNullFields() {
        PlainAccessConfig c1 = new PlainAccessConfig();
        PlainAccessConfig c2 = new PlainAccessConfig();
        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
    }

    @Test
    public void testToString() {
        PlainAccessConfig config = new PlainAccessConfig();
        config.setAccessKey("ak");
        config.setAdmin(true);
        String str = config.toString();
        assertTrue(str.contains("ak"));
        assertTrue(str.contains("admin=true"));
    }
}
