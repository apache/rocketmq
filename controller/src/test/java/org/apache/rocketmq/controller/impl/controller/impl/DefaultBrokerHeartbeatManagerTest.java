package org.apache.rocketmq.controller.impl.controller.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.impl.DefaultBrokerHeartbeatManager;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DefaultBrokerHeartbeatManagerTest {
    private BrokerHeartbeatManager heartbeatManager;

    @Before
    public void init() {
        final ControllerConfig config = new ControllerConfig();
        config.setScanNotActiveBrokerInterval(2000);
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(config);
        this.heartbeatManager.start();
    }

    @Test
    public void testDetectBrokerAlive() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        this.heartbeatManager.addBrokerLifecycleListener((clusterName, brokerAddress, brokerId) -> {
            System.out.println("Broker shutdown:" + brokerAddress);
            latch.countDown();
        });
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:7000", 1L, 3000L, null);
        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
        this.heartbeatManager.shutdown();
    }

}