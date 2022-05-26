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

package org.apache.rocketmq.namesrv.route;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.route.NearbyRoute;
import org.apache.rocketmq.common.route.Network;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class NearbyRouteManagerTest {

    @BeforeClass
    public static void setup() {
    	NearbyRoute nearbyRoute = new NearbyRoute();
    	nearbyRoute.setNearbyRoute(true);
    	nearbyRoute.setWhiteRemoteAddresses(Arrays.asList("192.168.1.*"));
    	Network network1 = new Network();
    	network1.setTag("vdc1");
    	network1.setSubnet(Arrays.asList("182.100.*"));
    	Network network2 = new Network();
    	network2.setTag("vdc2");
    	network2.setSubnet(Arrays.asList("172.100.*"));
    	nearbyRoute.setNetworks(Arrays.asList(network1, network2));
    	NearbyRouteManager.INSTANCE.decode(new Yaml().dump(nearbyRoute));
    	
    }

    @AfterClass
    public static void terminate() {
    	
    }

    @Test
    public void testIsNearbyRoute() {
    	 assertThat(NearbyRouteManager.INSTANCE.isNearbyRoute()).isTrue();
    }
    
    @Test
    public void testIsWhiteRemoteAddresses() {
    	assertThat(NearbyRouteManager.INSTANCE.isWhiteRemoteAddresses("192.168.1.100")).isTrue();
    	assertThat(NearbyRouteManager.INSTANCE.isWhiteRemoteAddresses("192.168.2.100")).isFalse();
    }
    @Test
    public void testFilter() {
    	TopicRouteData topicRouteData = new TopicRouteData();
    	
    	HashMap<Long, String> brokerA =  new HashMap<>();
    	brokerA.put(MixAll.MASTER_ID, "182.100.166.11:10911");
    	brokerA.put(Long.valueOf("1"), "172.100.126.1:10911");
    	
    	QueueData brokerAQueueData = new QueueData();
    	brokerAQueueData.setBrokerName("broker-a");
    	brokerAQueueData.setReadQueueNums(16);
    	brokerAQueueData.setWriteQueueNums(16);
    	brokerAQueueData.setPerm(6);
    	
    	HashMap<Long, String> brokerB =  new HashMap<>();
    	brokerB.put(MixAll.MASTER_ID, "172.100.1.11:10911");
    	brokerB.put(Long.valueOf("1"), "182.100.6.2:10911");
    	
    	QueueData brokerBQueueData = new QueueData();
    	brokerBQueueData.setBrokerName("broker-b");
    	brokerBQueueData.setReadQueueNums(16);
    	brokerBQueueData.setWriteQueueNums(16);
    	brokerBQueueData.setPerm(6);
    	
    	topicRouteData.setBrokerDatas(Arrays.asList(new BrokerData("DefaultCluster", "broker-a", brokerA), new BrokerData("DefaultCluster", "broker-b", brokerB)));
    	topicRouteData.setQueueDatas(Arrays.asList(brokerAQueueData, brokerBQueueData));
    	
    	//Remote address is not included in all network strategy, Do not filter.
    	TopicRouteData topicRouteData2 = NearbyRouteManager.INSTANCE.filter("152.168.1.1:10000", topicRouteData);
    	assertThat(topicRouteData2).isEqualTo(topicRouteData);
    	//filter by network strategy
    	topicRouteData2 = NearbyRouteManager.INSTANCE.filter("182.100.1.1:10000", topicRouteData);
    	assertThat(topicRouteData2.getBrokerDatas()).isEqualTo(Arrays.asList(new BrokerData("DefaultCluster", "broker-a", brokerA)));
    	assertThat(topicRouteData2.getQueueDatas()).isEqualTo(Arrays.asList(brokerAQueueData));
    }
    
}