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

package org.apache.rocketmq.proxy.service.admin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultAdminServiceTest {
    @Mock
    private MQClientAPIFactory mqClientAPIFactory;
    @Mock
    private MQClientAPIExt mqClientAPIExt;

    private DefaultAdminService defaultAdminService;

    @Before
    public void before() {
        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);
        defaultAdminService = new DefaultAdminService(mqClientAPIFactory);
    }

    @Test
    public void testCreateTopic() throws Exception {
        when(mqClientAPIExt.getTopicRouteInfoFromNameServer(eq("createTopic"), anyLong()))
            .thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""))
            .thenReturn(createTopicRouteData(1));
        when(mqClientAPIExt.getTopicRouteInfoFromNameServer(eq("sampleTopic"), anyLong()))
            .thenReturn(createTopicRouteData(2));

        ArgumentCaptor<String> addrArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TopicConfig> topicConfigArgumentCaptor = ArgumentCaptor.forClass(TopicConfig.class);
        doNothing().when(mqClientAPIExt).createTopic(addrArgumentCaptor.capture(), anyString(), topicConfigArgumentCaptor.capture(), anyLong());

        assertTrue(defaultAdminService.createTopicOnTopicBrokerIfNotExist(
            "createTopic",
            "sampleTopic",
            7,
            8,
            true,
            1
        ));

        assertEquals(2, addrArgumentCaptor.getAllValues().size());
        Set<String> createAddr = new HashSet<>(addrArgumentCaptor.getAllValues());
        assertTrue(createAddr.contains("127.0.0.1:10911"));
        assertTrue(createAddr.contains("127.0.0.2:10911"));
        assertEquals("createTopic", topicConfigArgumentCaptor.getValue().getTopicName());
        assertEquals(7, topicConfigArgumentCaptor.getValue().getWriteQueueNums());
        assertEquals(8, topicConfigArgumentCaptor.getValue().getReadQueueNums());
    }

    private TopicRouteData createTopicRouteData(int brokerNum) {
        TopicRouteData topicRouteData = new TopicRouteData();
        for (int i = 0; i < brokerNum; i++) {
            BrokerData brokerData = new BrokerData();
            HashMap<Long, String> addrMap = new HashMap<>();
            addrMap.put(0L, "127.0.0." + (i + 1) + ":10911");
            brokerData.setBrokerAddrs(addrMap);
            brokerData.setBrokerName("broker-" + i);
            brokerData.setCluster("cluster");
            topicRouteData.getBrokerDatas().add(brokerData);
        }
        return topicRouteData;
    }
}