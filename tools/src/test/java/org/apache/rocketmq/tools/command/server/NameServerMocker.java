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
package org.apache.rocketmq.tools.command.server;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * tools class
 */
public class NameServerMocker {

    /**
     * use the specified port to start the nameserver
     *
     * @param nameServerPort    nameServer port
     * @param brokerPort    broker port
     * @return  ServerResponseMocker
     */
    public static ServerResponseMocker startByDefaultConf(int nameServerPort, int brokerPort) {
        return startByDefaultConf(nameServerPort, brokerPort, null);
    }

    /**
     * use the specified port to start the nameserver
     *
     * @param nameServerPort    nameServer port
     * @param brokerPort    broker port
     * @param extMap    extend config
     * @return  ServerResponseMocker
     */
    public static ServerResponseMocker startByDefaultConf(int nameServerPort, int brokerPort,
                                                          HashMap<String, String> extMap) {

        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:" + nameServerPort);
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> dataList = new ArrayList<>();
        HashMap<Long, String> brokerAddress = new HashMap<>();
        brokerAddress.put(1L, "127.0.0.1:" + brokerPort);
        BrokerData brokerData = new BrokerData("mockCluster", "mockBrokerName", brokerAddress);
        brokerData.setBrokerName("mockBrokerName");
        dataList.add(brokerData);
        topicRouteData.setBrokerDatas(dataList);
        // start name server
        return ServerResponseMocker.startServer(nameServerPort, topicRouteData.encode(), extMap);
    }

}
