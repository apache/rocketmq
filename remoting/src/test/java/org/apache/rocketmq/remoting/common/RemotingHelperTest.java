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
package org.apache.rocketmq.remoting.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemotingHelperTest {

    @Test
    public void testIpInCIDRWithZeroPrefixMatchesEveryAddress() {
        // A /0 prefix is the catch-all CIDR and must match every IPv4 address,
        // mirroring IPAddressUtils.isIPInRange(ip, "0.0.0.0/0").
        assertThat(RemotingHelper.ipInCIDR("0.0.0.0", "0.0.0.0/0")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("10.0.0.1", "0.0.0.0/0")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.1.1", "0.0.0.0/0")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("255.255.255.255", "0.0.0.0/0")).isTrue();
    }

    @Test
    public void testIpInCIDRWithFullPrefixRequiresExactAddress() {
        assertThat(RemotingHelper.ipInCIDR("192.168.1.1", "192.168.1.1/32")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.1.2", "192.168.1.1/32")).isFalse();
    }

    @Test
    public void testIpInCIDRWithNormalPrefix() {
        assertThat(RemotingHelper.ipInCIDR("192.168.1.100", "192.168.1.0/24")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.2.1", "192.168.1.0/24")).isFalse();
        assertThat(RemotingHelper.ipInCIDR("10.1.2.3", "10.0.0.0/8")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("11.1.2.3", "10.0.0.0/8")).isFalse();
    }

    @Test
    public void testIpInCIDRMasksHostBitsOfCidr() {
        // Host bits of the CIDR must be ignored: 192.168.1.5/24 behaves like 192.168.1.0/24.
        assertThat(RemotingHelper.ipInCIDR("192.168.1.200", "192.168.1.5/24")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.2.200", "192.168.1.5/24")).isFalse();
    }
}