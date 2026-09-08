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
    public void testIpInCidrMatchesEverythingForZeroPrefixLength() {
        assertThat(RemotingHelper.ipInCIDR("1.2.3.4", "0.0.0.0/0")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("8.8.8.8", "10.0.0.0/0")).isTrue();
    }

    @Test
    public void testIpInCidrForRegularPrefixLengths() {
        assertThat(RemotingHelper.ipInCIDR("192.168.1.7", "192.168.1.0/24")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.2.7", "192.168.1.0/24")).isFalse();
        assertThat(RemotingHelper.ipInCIDR("192.0.2.0", "192.0.2.0/32")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.0.2.2", "192.0.2.0/32")).isFalse();
    }
}
