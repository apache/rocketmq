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

import java.time.Duration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProxyBrokerHeartbeatConfigTest {

    @Test
    public void testHeartbeatDefaultsKeepIdleBrokerChannelsAlive() {
        ProxyConfig proxyConfig = new ProxyConfig();

        assertTrue(proxyConfig.isEnableProxyBrokerHeartbeat());
        assertEquals(Duration.ofSeconds(30).toMillis(),
            proxyConfig.getProxyBrokerHeartbeatIntervalMillis());
        assertEquals(Duration.ofSeconds(3).toMillis(),
            proxyConfig.getProxyBrokerHeartbeatTimeoutMillis());
    }

    @Test
    public void testHeartbeatConfigurationSetters() {
        ProxyConfig proxyConfig = new ProxyConfig();

        proxyConfig.setEnableProxyBrokerHeartbeat(false);
        proxyConfig.setProxyBrokerHeartbeatIntervalMillis(45_000);
        proxyConfig.setProxyBrokerHeartbeatTimeoutMillis(5_000);

        assertFalse(proxyConfig.isEnableProxyBrokerHeartbeat());
        assertEquals(45_000, proxyConfig.getProxyBrokerHeartbeatIntervalMillis());
        assertEquals(5_000, proxyConfig.getProxyBrokerHeartbeatTimeoutMillis());
    }

    @Test
    public void testEnabledHeartbeatRejectsZeroInterval() {
        ProxyConfig proxyConfig = validProxyConfig();
        proxyConfig.setProxyBrokerHeartbeatIntervalMillis(0);

        assertThatThrownBy(proxyConfig::initData)
            .hasMessageContaining("proxyBrokerHeartbeatIntervalMillis must be greater than zero");
    }

    @Test
    public void testEnabledHeartbeatRejectsNegativeInterval() {
        ProxyConfig proxyConfig = validProxyConfig();
        proxyConfig.setProxyBrokerHeartbeatIntervalMillis(-1);

        assertThatThrownBy(proxyConfig::initData)
            .hasMessageContaining("proxyBrokerHeartbeatIntervalMillis must be greater than zero");
    }

    @Test
    public void testEnabledHeartbeatRejectsZeroTimeout() {
        ProxyConfig proxyConfig = validProxyConfig();
        proxyConfig.setProxyBrokerHeartbeatTimeoutMillis(0);

        assertThatThrownBy(proxyConfig::initData)
            .hasMessageContaining("proxyBrokerHeartbeatTimeoutMillis must be greater than zero");
    }

    @Test
    public void testEnabledHeartbeatRejectsNegativeTimeout() {
        ProxyConfig proxyConfig = validProxyConfig();
        proxyConfig.setProxyBrokerHeartbeatTimeoutMillis(-1);

        assertThatThrownBy(proxyConfig::initData)
            .hasMessageContaining("proxyBrokerHeartbeatTimeoutMillis must be greater than zero");
    }

    @Test
    public void testDisabledHeartbeatAllowsUnusedTimingValues() {
        ProxyConfig proxyConfig = validProxyConfig();
        proxyConfig.setEnableProxyBrokerHeartbeat(false);
        proxyConfig.setProxyBrokerHeartbeatIntervalMillis(0);
        proxyConfig.setProxyBrokerHeartbeatTimeoutMillis(0);

        proxyConfig.initData();

        assertFalse(proxyConfig.isEnableProxyBrokerHeartbeat());
    }

    private ProxyConfig validProxyConfig() {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setLocalServeAddr("127.0.0.1");
        return proxyConfig;
    }
}
