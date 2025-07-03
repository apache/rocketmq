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

import static org.apache.rocketmq.common.utils.NetworkUtil.validCommonInet6Address;

public class IPAddressUtilsTest {

    @Test
    public void isIPInRange() {

        // IPv4 test
        String ipv4Address = "192.168.1.10";
        String ipv4Cidr = "192.168.1.0/24";
        assert IPAddressUtils.isIPInRange(ipv4Address, ipv4Cidr);

        ipv4Address = "192.168.2.10";
        assert !IPAddressUtils.isIPInRange(ipv4Address, ipv4Cidr);

        // IPv6 test
        String ipv6Address = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
        String ipv6Cidr = "2001:0db8:85a3::/48";
        assert IPAddressUtils.isIPInRange(ipv6Address, ipv6Cidr);
    }

    @Test
    public void isValidCidr() {
        String ipv4Cidr = "192.168.1.0/24";
        String ipv6Cidr = "2001:0db8:1234:5678::/64";
        String invalidCidr = "192.168.1.0";

        assert IPAddressUtils.isValidCidr(ipv4Cidr);
        assert IPAddressUtils.isValidCidr(ipv6Cidr);
        assert !IPAddressUtils.isValidCidr(invalidCidr);
    }

    @Test
    public void isValidIp() {
        String ipv4 = "192.168.1.0";
        String ipv6 = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
        String invalidIp = "192.168.1.256";
        String ipv4Cidr = "192.168.1.0/24";

        assert IPAddressUtils.isValidIp(ipv4);
        assert IPAddressUtils.isValidIp(ipv6);
        assert !IPAddressUtils.isValidIp(invalidIp);
        assert !IPAddressUtils.isValidIp(ipv4Cidr);
    }

    @Test
    public void isValidIPOrCidr() {
        String ipv4 = "192.168.1.0";
        String ipv6 = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
        String ipv4Cidr = "192.168.1.0/24";
        String ipv6Cidr = "2001:0db8:1234:5678::/64";
        assert IPAddressUtils.isValidIPOrCidr(ipv4);
        assert IPAddressUtils.isValidIPOrCidr(ipv6);
        assert IPAddressUtils.isValidIPOrCidr(ipv4Cidr);
        assert IPAddressUtils.isValidIPOrCidr(ipv6Cidr);
    }

    @Test
    public void isValidIPv6Common() {
        String ipv6WithoutScope = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
        assert validCommonInet6Address(ipv6WithoutScope);
        String ipv6WithScope = "2001:0db8:85a3:0000:0000:8a2e:0370:7334%eth0";
        assert validCommonInet6Address(ipv6WithScope);
        String ipv6WithBracketedAndScope = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334%eth0]";
        assert validCommonInet6Address(ipv6WithBracketedAndScope);
        String ipv4 = "192.168.1.0";
        assert !validCommonInet6Address(ipv4);
        String ipv4Cidr = "192.168.1.0/24";
        assert !validCommonInet6Address(ipv4Cidr);
    }

}