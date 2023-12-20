package org.apache.rocketmq.common.utils;

import org.junit.Test;

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
}