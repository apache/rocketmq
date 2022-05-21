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

package org.apache.rocketmq.thinclient.route;

import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

public class EndpointsTest {
    @Test
    public void testEndpointsWithSingleIpv4AndPort() {
        final Endpoints endpoints = new Endpoints("127.0.0.1:8080");
        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals(1, endpoints.getAddresses().size());
        final Address address = endpoints.getAddresses().iterator().next();
        Assert.assertEquals("127.0.0.1", address.getHost());
        Assert.assertEquals(8080, address.getPort());

        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals("ipv4:127.0.0.1:8080", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithSingleIpv4() {
        final Endpoints endpoints = new Endpoints("127.0.0.1");
        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals(1, endpoints.getAddresses().size());
        final Address address = endpoints.getAddresses().iterator().next();
        Assert.assertEquals("127.0.0.1", address.getHost());
        Assert.assertEquals(80, address.getPort());

        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals("ipv4:127.0.0.1:80", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithMultipleIpv4() {
        final Endpoints endpoints = new Endpoints("127.0.0.1:8080;127.0.0.2:8081");
        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals(2, endpoints.getAddresses().size());
        final Iterator<Address> iterator = endpoints.getAddresses().iterator();

        final Address address0 = iterator.next();
        Assert.assertEquals("127.0.0.1", address0.getHost());
        Assert.assertEquals(8080, address0.getPort());

        final Address address1 = iterator.next();
        Assert.assertEquals("127.0.0.2", address1.getHost());
        Assert.assertEquals(8081, address1.getPort());

        Assert.assertEquals(AddressScheme.IPv4, endpoints.getScheme());
        Assert.assertEquals("ipv4:127.0.0.1:8080,127.0.0.2:8081", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithSingleIpv6() {
        final Endpoints endpoints = new Endpoints("1050:0000:0000:0000:0005:0600:300c:326b:8080");
        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals(1, endpoints.getAddresses().size());
        final Address address = endpoints.getAddresses().iterator().next();
        Assert.assertEquals("1050:0000:0000:0000:0005:0600:300c:326b", address.getHost());
        Assert.assertEquals(8080, address.getPort());

        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals("ipv6:1050:0000:0000:0000:0005:0600:300c:326b:8080", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithMultipleIpv6() {
        final Endpoints endpoints = new Endpoints("1050:0000:0000:0000:0005:0600:300c:326b:8080;1050:0000:0000:0000:0005:0600:300c:326c:8081");
        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals(2, endpoints.getAddresses().size());
        final Iterator<Address> iterator = endpoints.getAddresses().iterator();

        final Address address0 = iterator.next();
        Assert.assertEquals("1050:0000:0000:0000:0005:0600:300c:326b", address0.getHost());
        Assert.assertEquals(8080, address0.getPort());

        final Address address1 = iterator.next();
        Assert.assertEquals("1050:0000:0000:0000:0005:0600:300c:326c", address1.getHost());
        Assert.assertEquals(8081, address1.getPort());

        Assert.assertEquals(AddressScheme.IPv6, endpoints.getScheme());
        Assert.assertEquals("ipv6:1050:0000:0000:0000:0005:0600:300c:326b:8080,1050:0000:0000:0000:0005:0600:300c:326c:8081", endpoints.getFacade());
    }

    @Test
    public void testEndpointsWithDomain() {
        final Endpoints endpoints = new Endpoints("rocketmq.apache.org");
        Assert.assertEquals(AddressScheme.DOMAIN_NAME, endpoints.getScheme());
        final Iterator<Address> iterator = endpoints.getAddresses().iterator();

        final Address address = iterator.next();
        Assert.assertEquals("rocketmq.apache.org", address.getHost());
        Assert.assertEquals(80, address.getPort());
    }

    @Test
    public void testEndpointsWithDomainAndPort() {
        final Endpoints endpoints = new Endpoints("rocketmq.apache.org:8081");
        Assert.assertEquals(AddressScheme.DOMAIN_NAME, endpoints.getScheme());
        final Iterator<Address> iterator = endpoints.getAddresses().iterator();

        final Address address = iterator.next();
        Assert.assertEquals("rocketmq.apache.org", address.getHost());
        Assert.assertEquals(8081, address.getPort());
    }
}