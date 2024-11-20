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