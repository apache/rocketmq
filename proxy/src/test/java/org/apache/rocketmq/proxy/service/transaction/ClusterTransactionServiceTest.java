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

package org.apache.rocketmq.proxy.service.transaction;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;


public class ClusterTransactionServiceTest extends BaseServiceTest {

    @Mock
    private ProducerManager producerManager;

    private ClusterTransactionService clusterTransactionService;

    @Before
    public void before() throws Throwable {
        super.before();
        this.clusterTransactionService = new ClusterTransactionService(this.topicRouteService, this.producerManager, null,
            this.mqClientAPIFactory);

        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData);
        when(this.topicRouteService.getAllMessageQueueView(anyString()))
            .thenReturn(messageQueueView);

        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);
    }

    @Test
    public void testAddTransactionSubscription() {
        this.clusterTransactionService.addTransactionSubscription(GROUP, TOPIC);

        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME, this.clusterTransactionService.getGroupClusterData().get(GROUP).stream().findAny().get().getCluster());
    }

    @Test
    public void testAddTransactionSubscriptionTopicList() {
        this.clusterTransactionService.addTransactionSubscription(GROUP, Lists.newArrayList(TOPIC + 1, TOPIC + 2));

        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME, this.clusterTransactionService.getGroupClusterData().get(GROUP).stream().findAny().get().getCluster());
    }

    @Test
    public void testReplaceTransactionSubscription() {
        this.clusterTransactionService.addTransactionSubscription(GROUP, TOPIC);

        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME, this.clusterTransactionService.getGroupClusterData().get(GROUP).stream().findAny().get().getCluster());

        this.brokerData.setCluster(CLUSTER_NAME + 1);
        this.clusterTransactionService.replaceTransactionSubscription(GROUP, Lists.newArrayList(TOPIC + 1));
        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME + 1, this.clusterTransactionService.getGroupClusterData().get(GROUP).stream().findAny().get().getCluster());
    }

    @Test
    public void testUnSubscribeAllTransactionTopic() {
        this.clusterTransactionService.addTransactionSubscription(GROUP, TOPIC);
        this.clusterTransactionService.unSubscribeAllTransactionTopic(GROUP);

        assertEquals(0, this.clusterTransactionService.getGroupClusterData().size());
    }

    @Test
    public void testScanProducerHeartBeat() throws Exception {
        ConfigurationManager.getProxyConfig().setTransactionHeartbeatBatchNum(2);
        this.clusterTransactionService.start();
        Set<String> groupSet = new HashSet<>();

        for (int i = 0; i < 3; i++) {
            groupSet.add(GROUP + i);
            this.clusterTransactionService.addTransactionSubscription(GROUP + i, TOPIC);
        }

        ArgumentCaptor<String> brokerAddrArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HeartbeatData> heartbeatDataArgumentCaptor = ArgumentCaptor.forClass(HeartbeatData.class);
        doNothing().when(mqClientAPIExt).sendHeartbeatOneway(
            brokerAddrArgumentCaptor.capture(),
            heartbeatDataArgumentCaptor.capture(),
            anyLong()
        );

        this.clusterTransactionService.scanProducerHeartBeat();

        await().atMost(Duration.ofSeconds(1)).until(() -> brokerAddrArgumentCaptor.getAllValues().size() == 2);

        assertEquals(Lists.newArrayList(BROKER_ADDR, BROKER_ADDR), brokerAddrArgumentCaptor.getAllValues());
        List<HeartbeatData> heartbeatDataList = heartbeatDataArgumentCaptor.getAllValues();
        for (ProducerData producerData : heartbeatDataList.get(0).getProducerDataSet()) {
            groupSet.remove(producerData.getGroupName());
        }

        for (ProducerData producerData : heartbeatDataList.get(1).getProducerDataSet()) {
            groupSet.remove(producerData.getGroupName());
        }

        assertTrue(groupSet.isEmpty());
    }
}