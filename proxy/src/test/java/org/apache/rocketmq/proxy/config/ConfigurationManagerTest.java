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

package org.apache.rocketmq.proxy.config;

import org.apache.rocketmq.proxy.ProxyMode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConfigurationManagerTest extends InitConfigTest {

    @Test
    public void testIntConfig() {
        assertThat(ConfigurationManager.getProxyConfig()).isNotNull();
        assertThat(ConfigurationManager.getProxyConfig().getProxyMode()).isEqualToIgnoringCase(ProxyMode.CLUSTER.toString());

        String brokerConfig = ConfigurationManager.getProxyConfig().getBrokerConfigPath();
        assertThat(brokerConfig).isEqualTo(ConfigurationManager.getProxyHome() + "/conf/broker.conf");
    }

    @Test
    public void testGetProxyHome() {
        // test configured proxy home
        assertThat(ConfigurationManager.getProxyHome()).isIn(mockProxyHome, "./");
    }

    @Test
    public void testGetProxyConfig() {
        assertThat(ConfigurationManager.getProxyConfig()).isNotNull();
    }

    @Test
    public void testFormatProxyConfig() {
        String actual = ConfigurationManager.formatProxyConfig();
        assertNotNull(actual);
        ProxyConfig expected = ConfigurationManager.getProxyConfig();
        assertTrue(actual.contains(expected.getProxyMode()));
        assertTrue(actual.contains(expected.getProxyName()));
        assertTrue(actual.contains(expected.getNamesrvAddr()));
    }
} 
