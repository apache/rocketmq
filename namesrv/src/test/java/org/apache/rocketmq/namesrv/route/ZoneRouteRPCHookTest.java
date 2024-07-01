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
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class ZoneRouteRPCHookTest {

    private ZoneRouteRPCHook zoneRouteRPCHook;

    @Before
    public void setup() {
        zoneRouteRPCHook = new ZoneRouteRPCHook();
    }

    @Test
    public void testDoAfterResponseWithNoZoneMode() {
        RemotingCommand request1 = RemotingCommand.createRequestCommand(106,null);
        zoneRouteRPCHook.doAfterResponse("", request1, null);

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(MixAll.ZONE_MODE, "false");
        RemotingCommand request = RemotingCommand.createRequestCommand(105,null);
        request.setExtFields(extFields);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setBody(RemotingSerializable.encode(createSampleTopicRouteData()));
        zoneRouteRPCHook.doAfterResponse("", request, response);
    }

    @Test
    public void testDoAfterResponseWithNoZoneName() {
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(MixAll.ZONE_MODE, "true");
        RemotingCommand request = RemotingCommand.createRequestCommand(105,null);
        request.setExtFields(extFields);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setBody(RemotingSerializable.encode(createSampleTopicRouteData()));
        zoneRouteRPCHook.doAfterResponse("", request, response);
    }

    @Test
    public void testDoAfterResponseWithNoResponse() {
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(MixAll.ZONE_MODE, "true");
        RemotingCommand request = RemotingCommand.createRequestCommand(105,null);
        request.setExtFields(extFields);
        zoneRouteRPCHook.doAfterResponse("", request, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        zoneRouteRPCHook.doAfterResponse("", request, response);

        response.setBody(RemotingSerializable.encode(createSampleTopicRouteData()));
        response.setCode(ResponseCode.NO_PERMISSION);
        zoneRouteRPCHook.doAfterResponse("", request, response);
    }


    @Test
    public void testDoAfterResponseWithValidZoneFiltering() throws Exception {
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(MixAll.ZONE_MODE, "true");
        extFields.put(MixAll.ZONE_NAME,"zone1");
        RemotingCommand request = RemotingCommand.createRequestCommand(105,null);
        request.setExtFields(extFields);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        TopicRouteData topicRouteData = createSampleTopicRouteData();
        response.setBody(RemotingSerializable.encode(topicRouteData));
        zoneRouteRPCHook.doAfterResponse("", request, response);

        HashMap<Long,String> brokeraddrs = new HashMap<>();
        brokeraddrs.put(MixAll.MASTER_ID,"127.0.0.1:10911");
        topicRouteData.getBrokerDatas().get(0).setBrokerAddrs(brokeraddrs);
        response.setBody(RemotingSerializable.encode(topicRouteData));
        zoneRouteRPCHook.doAfterResponse("", request, response);

        topicRouteData.getQueueDatas().add(createQueueData("BrokerB"));
        HashMap<Long,String> brokeraddrsB = new HashMap<>();
        brokeraddrsB.put(MixAll.MASTER_ID,"127.0.0.1:10912");
        BrokerData brokerData1 = createBrokerData("BrokerB","zone2",brokeraddrsB);
        BrokerData brokerData2 = createBrokerData("BrokerC","zone1",null);
        topicRouteData.getBrokerDatas().add(brokerData1);
        topicRouteData.getBrokerDatas().add(brokerData2);
        response.setBody(RemotingSerializable.encode(topicRouteData));
        zoneRouteRPCHook.doAfterResponse("", request, response);

        topicRouteData.getFilterServerTable().put("127.0.0.1:10911",new ArrayList<>());
        response.setBody(RemotingSerializable.encode(topicRouteData));
        zoneRouteRPCHook.doAfterResponse("", request, response);
        Assert.assertEquals(1,RemotingSerializable
                .decode(response.getBody(), TopicRouteData.class)
                .getFilterServerTable()
                .size());

        topicRouteData.getFilterServerTable().put("127.0.0.1:10912",new ArrayList<>());
        response.setBody(RemotingSerializable.encode(topicRouteData));
        zoneRouteRPCHook.doAfterResponse("", request, response);
        Assert.assertEquals(1,RemotingSerializable
                .decode(response.getBody(), TopicRouteData.class)
                .getFilterServerTable()
                .size());
    }

    private TopicRouteData createSampleTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDatas = new ArrayList<>();
        BrokerData brokerData = createBrokerData("BrokerA","zone1",new HashMap<>());
        List<QueueData> queueDatas = new ArrayList<>();
        QueueData queueData = createQueueData("BrokerA");
        queueDatas.add(queueData);
        brokerDatas.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(queueDatas);
        return topicRouteData;
    }

    private BrokerData createBrokerData(String brokerName,String zoneName,HashMap<Long,String> brokerAddrs){
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(brokerName);
        brokerData.setZoneName(zoneName);
        brokerData.setBrokerAddrs(brokerAddrs);
        return  brokerData;
    }

    private QueueData createQueueData(String brokerName){
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setReadQueueNums(8);
        queueData.setWriteQueueNums(8);
        queueData.setPerm(6);
        return queueData;
    }
}
