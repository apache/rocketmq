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
package org.apache.rocketmq.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AclConfigTest {

    @Test
    public void testGetGlobalWhiteAddrsWhenNull() {
        AclConfig aclConfig = new AclConfig();
        Assert.assertNull("The globalWhiteAddrs should return null", aclConfig.getGlobalWhiteAddrs());
    }

    @Test
    public void testGetGlobalWhiteAddrsWhenEmpty() {
        AclConfig aclConfig = new AclConfig();
        List<String> globalWhiteAddrs = new ArrayList<>();
        aclConfig.setGlobalWhiteAddrs(globalWhiteAddrs);
        assertNotNull("The globalWhiteAddrs should never return null", aclConfig.getGlobalWhiteAddrs());
        assertEquals("The globalWhiteAddrs list should be empty", 0, aclConfig.getGlobalWhiteAddrs().size());
    }

    @Test
    public void testGetGlobalWhiteAddrs() {
        AclConfig aclConfig = new AclConfig();
        List<String> expected = Arrays.asList("192.168.1.1", "192.168.1.2");
        aclConfig.setGlobalWhiteAddrs(expected);
        assertEquals("Global white addresses should match", expected, aclConfig.getGlobalWhiteAddrs());
        assertEquals("The globalWhiteAddrs list should be equal to 2", 2, aclConfig.getGlobalWhiteAddrs().size());
    }

    @Test
    public void testGetPlainAccessConfigsWhenNull() {
        AclConfig aclConfig = new AclConfig();
        Assert.assertNull("The plainAccessConfigs should return null", aclConfig.getPlainAccessConfigs());
    }

    @Test
    public void testGetPlainAccessConfigsWhenEmpty() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> plainAccessConfigs = new ArrayList<>();
        aclConfig.setPlainAccessConfigs(plainAccessConfigs);
        assertNotNull("The plainAccessConfigs should never return null", aclConfig.getPlainAccessConfigs());
        assertEquals("The plainAccessConfigs list should be empty", 0, aclConfig.getPlainAccessConfigs().size());
    }

    @Test
    public void testGetPlainAccessConfigs() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> expected = Arrays.asList(new PlainAccessConfig(), new PlainAccessConfig());
        aclConfig.setPlainAccessConfigs(expected);
        assertEquals("Plain access configs should match", expected, aclConfig.getPlainAccessConfigs());
        assertEquals("The plainAccessConfigs list should be equal to 2", 2, aclConfig.getPlainAccessConfigs().size());
    }

    @Test
    public void testToStringWithNullValues() {
        AclConfig aclConfig = new AclConfig();
        String result = aclConfig.toString();
        assertNotNull("toString should not be null", result);
        assertEquals("toString should match", "AclConfig{globalWhiteAddrs=null, plainAccessConfigs=null}", result);
    }

    @Test
    public void testToStringWithEmptyGlobalWhiteAddrsAndPlainAccessConfigs() {
        AclConfig aclConfig = new AclConfig();
        aclConfig.setGlobalWhiteAddrs(Collections.emptyList());
        aclConfig.setPlainAccessConfigs(Collections.emptyList());
        String expected = "AclConfig{globalWhiteAddrs=[], plainAccessConfigs=[]}";
        assertEquals(expected, aclConfig.toString());
    }
    
    @Test
    public void testToStringWithNonEmptyGlobalWhiteAddrsAndPlainAccessConfigs() {
        AclConfig aclConfig = new AclConfig();
        List<String> globalWhiteAddrs = Collections.singletonList("192.168.1.1");
        aclConfig.setGlobalWhiteAddrs(globalWhiteAddrs);
        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        List<PlainAccessConfig> plainAccessConfigs = Collections.singletonList(plainAccessConfig);
        aclConfig.setPlainAccessConfigs(plainAccessConfigs);
        String expected = "AclConfig{globalWhiteAddrs=[192.168.1.1], plainAccessConfigs=[" + plainAccessConfig + "]}";
        assertEquals("toString should match", expected, aclConfig.toString());
    }
}
