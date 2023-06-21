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

package org.apache.rocketmq.proxy.service;

import java.util.HashMap;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore
@RunWith(MockitoJUnitRunner.Silent.class)
public class BaseServiceTest extends InitConfigTest {

    protected TopicRouteService topicRouteService;
    protected MQClientAPIFactory mqClientAPIFactory;
    protected MQClientAPIExt mqClientAPIExt;

    protected static final String ERR_TOPIC = "errTopic";
    protected static final String TOPIC = "topic";
    protected static final String GROUP = "group";
    protected static final String BROKER_NAME = "broker";
    protected static final String CLUSTER_NAME = "cluster";
    protected static final String BROKER_ADDR = "127.0.0.1:10911";

    protected final TopicRouteData topicRouteData = new TopicRouteData();
    protected final QueueData queueData = new QueueData();
    protected final BrokerData brokerData = new BrokerData();

    @Before
    public void before() throws Throwable {
        super.before();

        topicRouteService = mock(TopicRouteService.class);
        mqClientAPIFactory = mock(MQClientAPIFactory.class);
        mqClientAPIExt = mock(MQClientAPIExt.class);
        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);

        queueData.setBrokerName(BROKER_NAME);
        topicRouteData.setQueueDatas(Lists.newArrayList(queueData));
        brokerData.setCluster(CLUSTER_NAME);
        brokerData.setBrokerName(BROKER_NAME);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, BROKER_ADDR);
        brokerData.setBrokerAddrs(brokerAddrs);
        topicRouteData.setBrokerDatas(Lists.newArrayList(brokerData));

        when(this.topicRouteService.getAllMessageQueueView(any(), eq(ERR_TOPIC))).thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""));
        when(this.topicRouteService.getAllMessageQueueView(any(), eq(TOPIC))).thenReturn(new MessageQueueView(TOPIC, topicRouteData));
        when(this.topicRouteService.getAllMessageQueueView(any(), eq(CLUSTER_NAME))).thenReturn(new MessageQueueView(CLUSTER_NAME, topicRouteData));
    }
}
