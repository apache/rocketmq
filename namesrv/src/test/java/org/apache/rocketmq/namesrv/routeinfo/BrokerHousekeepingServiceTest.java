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

package org.apache.rocketmq.namesrv.routeinfo;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BrokerHousekeepingServiceTest {
    private static BrokerHousekeepingService brokerHousekeepingService;

    @BeforeClass
    public static void setup() {
        NamesrvController namesrvController = new NamesrvController(
            new NamesrvConfig(),
            new NettyServerConfig()
        );
        brokerHousekeepingService = new BrokerHousekeepingService(namesrvController);
    }

    @AfterClass
    public static void terminate() {

    }

    @Test
    public void testOnChannelClose() {
        brokerHousekeepingService.onChannelClose("127.0.0.1:9876", null);
    }

    @Test
    public void testOnChannelException() {
        brokerHousekeepingService.onChannelException("127.0.0.1:9876", null);
    }

    @Test
    public void testOnChannelIdle() {
        brokerHousekeepingService.onChannelException("127.0.0.1:9876", null);
    }

}