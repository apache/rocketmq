package com.alibaba.rocketmq.broker;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nottyjay
 * @date 2016/12/25
 */
public class NameServerConnectorTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerControllerTest.class);

    @Test
    public void testConnectorWithDefaultPort() throws Exception {
        BrokerStartup.start(BrokerStartup.createBrokerController(new String[]{"-n", "localhost:9876"}));
        Thread.sleep(6000);
        BrokerStartup.createBrokerController(new String[]{"-c"});
    }

    @Test
    public void testConnectorWithOtherPort() throws Exception {
        BrokerStartup.start(BrokerStartup.createBrokerController(new String[]{"-n", "localhost:12345"}));
        Thread.sleep(6000);
        BrokerStartup.createBrokerController(new String[]{"-c"});
    }
}