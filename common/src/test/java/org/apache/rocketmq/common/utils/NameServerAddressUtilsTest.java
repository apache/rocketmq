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

package org.apache.rocketmq.common.utils;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NameServerAddressUtilsTest {

    private static String endpoint1 = "http://127.0.0.1:9876";
    private static String endpoint2 = "127.0.0.1:9876";
    private static String endpoint3
        = "http://MQ_INST_123456789_BXXUzaee.xxx:80";
    private static String endpoint4 = "MQ_INST_123456789_BXXUzaee.xxx:80";

    @Test
    public void testValidateInstanceEndpoint() {
        assertThat(NameServerAddressUtils.validateInstanceEndpoint(endpoint1)).isEqualTo(false);
        assertThat(NameServerAddressUtils.validateInstanceEndpoint(endpoint2)).isEqualTo(false);
        assertThat(NameServerAddressUtils.validateInstanceEndpoint(endpoint3)).isEqualTo(true);
        assertThat(NameServerAddressUtils.validateInstanceEndpoint(endpoint4)).isEqualTo(true);
    }

    @Test
    public void testParseInstanceIdFromEndpoint() {
        assertThat(NameServerAddressUtils.parseInstanceIdFromEndpoint(endpoint3)).isEqualTo(
            "MQ_INST_123456789_BXXUzaee");
        assertThat(NameServerAddressUtils.parseInstanceIdFromEndpoint(endpoint4)).isEqualTo(
            "MQ_INST_123456789_BXXUzaee");
    }

    @Test
    public void testGetNameSrvAddrFromNamesrvEndpoint() {
        assertThat(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint1))
            .isEqualTo("127.0.0.1:9876");
        assertThat(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint2))
            .isEqualTo("127.0.0.1:9876");
        assertThat(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint3))
            .isEqualTo("MQ_INST_123456789_BXXUzaee.xxx:80");
        assertThat(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint4))
            .isEqualTo("MQ_INST_123456789_BXXUzaee.xxx:80");
    }
}
