/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.route;

import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class LocalTopicRouteServiceTest extends BaseServiceTest {

    private static final String LOCAL_BROKER_NAME = "localBroker";
    private static final String LOCAL_CLUSTER_NAME = "localCluster";
    private static final String LOCAL_HOST = "127.0.0.2";
    private static final int LOCAL_PORT = 10911;
    private static final String LOCAL_ADDR = LOCAL_HOST + ":" + LOCAL_PORT;
    @Mock
    private BrokerController brokerController;
    @Mock
    private TopicConfigManager topicConfigManager;
    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
    private BrokerConfig brokerConfig = new BrokerConfig();
    private LocalTopicRouteService topicRouteService;

    @Before
    public void before() throws Throwable {
        super.before();
        this.brokerConfig.setBrokerName(LOCAL_BROKER_NAME);
        this.brokerConfig.setBrokerClusterName(LOCAL_CLUSTER_NAME);

        when(this.brokerController.getBrokerAddr()).thenReturn(LOCAL_ADDR);
        when(this.brokerController.getBrokerConfig()).thenReturn(this.brokerConfig);
        when(this.brokerController.getTopicConfigManager()).thenReturn(this.topicConfigManager);
        when(this.topicConfigManager.getTopicConfigTable()).thenReturn(this.topicConfigTable);

        this.topicRouteService = new LocalTopicRouteService(this.brokerController, this.mqClientAPIFactory);

        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(TOPIC), anyLong())).thenReturn(topicRouteData);
        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(ERR_TOPIC), anyLong())).thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""));
    }

    @Test
    public void testGetCurrentMessageQueueView() throws Throwable {
        ProxyContext ctx = ProxyContext.create();
        this.topicConfigTable.put(TOPIC, new TopicConfig(TOPIC, 3, 2, PermName.PERM_WRITE | PermName.PERM_READ));
        MessageQueueView messageQueueView = this.topicRouteService.getCurrentMessageQueueView(ctx, TOPIC);
        assertEquals(3, messageQueueView.getReadSelector().getQueues().size());
        assertEquals(2, messageQueueView.getWriteSelector().getQueues().size());
        assertEquals(1, messageQueueView.getReadSelector().getBrokerActingQueues().size());
        assertEquals(1, messageQueueView.getWriteSelector().getBrokerActingQueues().size());

        assertEquals(LOCAL_ADDR, messageQueueView.getReadSelector().selectOne(true).getBrokerAddr());
        assertEquals(LOCAL_BROKER_NAME, messageQueueView.getReadSelector().selectOne(true).getBrokerName());
        assertEquals(messageQueueView.getReadSelector().selectOne(true), messageQueueView.getWriteSelector().selectOne(true));
    }

    @Test
    public void testGetTopicRouteForProxy() throws Throwable {
        ProxyContext ctx = ProxyContext.create();
        ProxyTopicRouteData proxyTopicRouteData = this.topicRouteService.getTopicRouteForProxy(ctx, new ArrayList<>(), TOPIC);

        assertEquals(1, proxyTopicRouteData.getBrokerDatas().size());
        assertEquals(
            Lists.newArrayList(new Address(Address.AddressScheme.IPv4, HostAndPort.fromParts(
                HostAndPort.fromString(BROKER_ADDR).getHost(),
                ConfigurationManager.getProxyConfig().getGrpcServerPort()))),
            proxyTopicRouteData.getBrokerDatas().get(0).getBrokerAddrs().get(MixAll.MASTER_ID));
    }
}
