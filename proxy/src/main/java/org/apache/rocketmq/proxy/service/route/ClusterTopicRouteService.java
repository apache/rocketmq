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
package org.apache.rocketmq.proxy.service.route;

import java.util.List;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;

public class ClusterTopicRouteService extends TopicRouteService {

    public ClusterTopicRouteService(MQClientAPIFactory mqClientAPIFactory) {
        super(mqClientAPIFactory);
    }

    @Override
    public MessageQueueView getCurrentMessageQueueView(String topicName) throws Exception {
        return getAllMessageQueueView(topicName);
    }

    @Override
    public ProxyTopicRouteData getTopicRouteForProxy(List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        TopicRouteData topicRouteData = getAllMessageQueueView(topicName).getTopicRouteData();

        ProxyTopicRouteData proxyTopicRouteData = new ProxyTopicRouteData();
        proxyTopicRouteData.setQueueDatas(topicRouteData.getQueueDatas());

        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
            proxyBrokerData.setCluster(brokerData.getCluster());
            proxyBrokerData.setBrokerName(brokerData.getBrokerName());
            for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                proxyBrokerData.getBrokerAddrs().put(brokerId, requestHostAndPortList);
            }
            proxyTopicRouteData.getBrokerDatas().add(proxyBrokerData);
        }

        return proxyTopicRouteData;
    }

    @Override
    public String getBrokerAddr(String brokerName) throws Exception {
        List<BrokerData> brokerDataList = getAllMessageQueueView(brokerName).getTopicRouteData().getBrokerDatas();
        if (brokerDataList.isEmpty()) {
            return null;
        }
        return brokerDataList.get(0).getBrokerAddrs().get(MixAll.MASTER_ID);
    }

    @Override
    public AddressableMessageQueue buildAddressableMessageQueue(MessageQueue messageQueue) throws Exception {
        String brokerAddress = getBrokerAddr(messageQueue.getBrokerName());
        return new AddressableMessageQueue(messageQueue, brokerAddress);
    }
}
