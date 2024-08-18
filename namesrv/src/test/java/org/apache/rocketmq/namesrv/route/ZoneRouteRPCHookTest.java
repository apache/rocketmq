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


import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ZoneRouteRPCHookTest {

    private ZoneRouteRPCHook zoneRouteRPCHook;

    @Before
    public void setUp() {
        zoneRouteRPCHook = new ZoneRouteRPCHook();
    }

    @Test
    public void testFilterByZoneName_ValidInput_ShouldFilterCorrectly() {
        // Arrange
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(generateBrokerDataList());
        topicRouteData.setQueueDatas(generateQueueDataList());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC,null);
        request.setExtFields(new HashMap<String, String>() {{
            put(MixAll.ZONE_MODE, "true");
            put(MixAll.ZONE_NAME, "ZoneA");
        }});

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "remark");

        // Act
        zoneRouteRPCHook.doAfterResponse("127.0.0.1", request, response);
        TopicRouteData decodedResponse = RemotingSerializable.decode(response.getBody(), TopicRouteData.class);

        // Assert
        assertNull(decodedResponse);
    }

    @Test
    public void testFilterByZoneName_NoZoneName_ShouldNotFilter() {
        // Arrange
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(generateBrokerDataList());
        topicRouteData.setQueueDatas(generateQueueDataList());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC,null);
        request.setExtFields(new HashMap<String, String>() {{
            put(MixAll.ZONE_MODE, "true");
        }});

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);

        // Act
        zoneRouteRPCHook.doAfterResponse("127.0.0.1", request, response);
        TopicRouteData decodedResponse = RemotingSerializable.decode(response.getBody(), TopicRouteData.class);

        // Assert
        assertEquals(topicRouteData.getBrokerDatas().size(), 2);
        assertEquals(topicRouteData.getQueueDatas().size(), 2);
    }

    @Test
    public void testFilterByZoneName_ZoneModeFalse_ShouldNotFilter() {
        // Arrange
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(generateBrokerDataList());
        topicRouteData.setQueueDatas(generateQueueDataList());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC,null);
        request.setExtFields(new HashMap<String, String>() {{
            put(MixAll.ZONE_MODE, "false");
            put(MixAll.ZONE_NAME, "ZoneA");
        }});

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS ,null);

        // Act
        zoneRouteRPCHook.doAfterResponse("127.0.0.1", request, response);
        TopicRouteData decodedResponse = RemotingSerializable.decode(response.getBody(), TopicRouteData.class);

        // Assert
        assertEquals(topicRouteData.getBrokerDatas().size(), 2);
        assertEquals(topicRouteData.getQueueDatas().size(), 2);
    }

    private List<BrokerData> generateBrokerDataList() {
        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("BrokerA");
        brokerData1.setZoneName("ZoneA");
        Map<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:10911");
        brokerData1.setBrokerAddrs((HashMap<Long, String>) brokerAddrs);
        brokerDataList.add(brokerData1);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("BrokerB");
        brokerData2.setZoneName("ZoneB");
        brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:10912");
        brokerData2.setBrokerAddrs((HashMap<Long, String>) brokerAddrs);
        brokerDataList.add(brokerData2);

        return brokerDataList;
    }

    private List<QueueData> generateQueueDataList() {
        List<QueueData> queueDataList = new ArrayList<>();
        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("BrokerA");
        queueDataList.add(queueData1);

        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("BrokerB");
        queueDataList.add(queueData2);

        return queueDataList;
    }
}