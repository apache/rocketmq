package org.apache.rocketmq.common.utils;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NameServerAddressUtilsTest {

    private static String endpoint1 = "http://127.0.0.1:9876";
    private static String endpoint2 = "127.0.0.1:9876";
    private static String endpoint3
        = "http://MQ_INST_123456789_BXXUzaee.mq-internet-access.mq-internet.aliyuncs.com:80";
    private static String endpoint4 = "MQ_INST_123456789_BXXUzaee.mq-internet-access.mq-internet.aliyuncs.com:80";

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
            .isEqualTo("MQ_INST_123456789_BXXUzaee.mq-internet-access.mq-internet.aliyuncs.com:80");
        assertThat(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint4))
            .isEqualTo("MQ_INST_123456789_BXXUzaee.mq-internet-access.mq-internet.aliyuncs.com:80");
    }
}
