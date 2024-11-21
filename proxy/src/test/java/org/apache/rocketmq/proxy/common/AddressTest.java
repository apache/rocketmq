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
package org.apache.rocketmq.proxy.common;

import com.google.common.net.HostAndPort;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
public class AddressTest {

    @Test
    public void testConstructorWithIPv4() {
        HostAndPort hostAndPort = HostAndPort.fromString("192.168.1.1:8080");
        Address address = new Address(hostAndPort);

        assertEquals(Address.AddressScheme.IPv4, address.getAddressScheme());
        assertEquals(hostAndPort, address.getHostAndPort());
    }

    @Test
    public void testConstructorWithIPv6() {
        HostAndPort hostAndPort = HostAndPort.fromString("[2001:db8::1]:8080");
        Address address = new Address(hostAndPort);

        assertEquals(Address.AddressScheme.IPv6, address.getAddressScheme());
        assertEquals(hostAndPort, address.getHostAndPort());
    }

    @Test
    public void testConstructorWithDomainName() {
        HostAndPort hostAndPort = HostAndPort.fromString("example.com:8080");
        Address address = new Address(hostAndPort);

        assertEquals(Address.AddressScheme.DOMAIN_NAME, address.getAddressScheme());
        assertEquals(hostAndPort, address.getHostAndPort());
    }

    @Test
    public void testConstructorWithNullHostAndPort() {
        Address address = new Address(null);

        assertEquals(Address.AddressScheme.UNRECOGNIZED, address.getAddressScheme());
        assertNull(address.getHostAndPort());
    }
}