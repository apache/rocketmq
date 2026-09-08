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
package org.apache.rocketmq.remoting.protocol.route;

import java.util.HashMap;
import org.apache.rocketmq.common.MixAll;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerDataTest {

    @Test
    public void testSelectBrokerAddrWithoutRegisteredBroker() {
        BrokerData brokerData = new BrokerData();

        assertThat(brokerData.selectBrokerAddr()).isNull();

        brokerData.setBrokerAddrs(new HashMap<>());
        assertThat(brokerData.selectBrokerAddr()).isNull();
    }

    @Test
    public void testSelectBrokerAddrPrefersMaster() {
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:10911");
        brokerAddrs.put(1L, "127.0.0.1:10912");

        BrokerData brokerData = new BrokerData("cluster", "broker", brokerAddrs);

        assertThat(brokerData.selectBrokerAddr()).isEqualTo("127.0.0.1:10911");
    }

    @Test
    public void testSelectBrokerAddrFallsBackToSlave() {
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(1L, "127.0.0.1:10912");

        BrokerData brokerData = new BrokerData("cluster", "broker", brokerAddrs);

        assertThat(brokerData.selectBrokerAddr()).isEqualTo("127.0.0.1:10912");
    }
}
