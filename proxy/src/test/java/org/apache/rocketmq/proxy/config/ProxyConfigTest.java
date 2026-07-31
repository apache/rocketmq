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

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProxyConfigTest {

    @Test
    public void initDataShouldResolveBlankLocalServeAddr() {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setLocalServeAddr(" \t ");
        proxyConfig.setRemotingAccessAddr(" ");

        proxyConfig.initData();

        assertThat(StringUtils.isBlank(proxyConfig.getLocalServeAddr())).isFalse();
        assertThat(proxyConfig.getRemotingAccessAddr()).isEqualTo(proxyConfig.getLocalServeAddr());
    }

    @Test
    public void testProxyAdminDefaults() {
        ProxyConfig config = new ProxyConfig();
        assertTrue(config.isProxyAdminEnabled());
        assertEquals(Integer.valueOf(8082), config.getProxyAdminServerPort());
        assertEquals(4, config.getProxyAdminThreadPoolNums());
        assertEquals(100, config.getProxyAdminMaxPageSize());
        assertEquals(8, config.getProxyAdminDescribeClientConcurrencyLimit());
        assertEquals(0.5, config.getProxyAdminSamplingRateUnderLoad(), 0.0001);
        assertEquals(10, config.getProxyAdminHeartbeatHistorySize());
        assertEquals(100000L, config.getProxyAdminSamplingThreshold());
    }

    @Test
    public void testProxyAdminSettersAndGetters() {
        ProxyConfig config = new ProxyConfig();

        config.setProxyAdminEnabled(false);
        assertFalse(config.isProxyAdminEnabled());

        config.setProxyAdminServerPort(9092);
        assertEquals(Integer.valueOf(9092), config.getProxyAdminServerPort());

        config.setProxyAdminThreadPoolNums(8);
        assertEquals(8, config.getProxyAdminThreadPoolNums());

        config.setProxyAdminMaxPageSize(50);
        assertEquals(50, config.getProxyAdminMaxPageSize());

        config.setProxyAdminDescribeClientConcurrencyLimit(16);
        assertEquals(16, config.getProxyAdminDescribeClientConcurrencyLimit());

        config.setProxyAdminSamplingRateUnderLoad(0.8);
        assertEquals(0.8, config.getProxyAdminSamplingRateUnderLoad(), 0.0001);

        config.setProxyAdminHeartbeatHistorySize(20);
        assertEquals(20, config.getProxyAdminHeartbeatHistorySize());

        config.setProxyAdminSamplingThreshold(200000L);
        assertEquals(200000L, config.getProxyAdminSamplingThreshold());
    }
}
