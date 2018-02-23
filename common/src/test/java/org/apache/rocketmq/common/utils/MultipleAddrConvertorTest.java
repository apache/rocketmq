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
package org.apache.rocketmq.common.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipleAddrConvertorTest {

    @Test
    public void testConvertAddr() {
        String multiTestAddr = "100.81.249.108:10911;127.0.0.1:80;127.0.0.1:22";
        String availableAddress = MultipleAddrConvertor.convert(multiTestAddr);
        if (availableAddress != null) {
            String[] addrs = availableAddress.split(";");
            assertThat(addrs.length).isEqualTo(1);
            String[] parts = availableAddress.split(":");
            assertThat(parts.length).isEqualTo(2);
        }
    }

    @Test
    public void testConvertTopicRouteData() {

        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "100.81.249.108:10911;127.0.0.1:80;127.0.0.1:22");
        brokerAddrs.put(1L, "100.81.249.108:10911");

        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("testBroker1");
        brokerData1.setBrokerAddrs(brokerAddrs);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("testBroker2");
        brokerData2.setBrokerAddrs(brokerAddrs);

        List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
        brokerDataList.add(brokerData1);
        brokerDataList.add(brokerData2);

        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(brokerDataList);

        TopicRouteData convertedRouteData = MultipleAddrConvertor.convert(topicRouteData);

        HashMap<Long, String> newBrokerAddrs = convertedRouteData.getBrokerDatas().get(0).getBrokerAddrs();
        if (newBrokerAddrs.get(0L) != null) {
            String[] addrs = newBrokerAddrs.get(0L).split(";");
            assertThat(addrs.length).isEqualTo(1);
            String[] parts = newBrokerAddrs.get(0L).split(":");
            assertThat(parts.length).isEqualTo(2);
        }
        assertThat(newBrokerAddrs.get(1L)).isEqualToIgnoringCase("100.81.249.108:10911");
    }

}
