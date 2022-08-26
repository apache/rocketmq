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
package org.apache.rocketmq.controller.impl.controller.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ControllerConfig;
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
        this.heartbeatManager.addBrokerLifecycleListener((clusterName, brokerName, brokerAddress, brokerId) -> {
            System.out.println("Broker shutdown:" + brokerAddress);
            latch.countDown();
        });
        this.heartbeatManager.registerBroker("cluster1", "broker1", "127.0.0.1:7000", 1L, 3000L, null, 1, 1L);
        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
        this.heartbeatManager.shutdown();
    }

}