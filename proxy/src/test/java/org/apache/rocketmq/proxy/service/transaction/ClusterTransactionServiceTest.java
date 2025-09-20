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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class ClusterTransactionServiceTest extends BaseServiceTest {

    @Mock
    private ProducerManager producerManager;
    private ProxyContext ctx = ProxyContext.create();
    private ClusterTransactionService clusterTransactionService;

    @Before
    public void before() throws Throwable {
        super.before();
        this.clusterTransactionService = new ClusterTransactionService(this.topicRouteService, this.producerManager,
            this.mqClientAPIFactory);

        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData, null);
        when(this.topicRouteService.getAllMessageQueueView(any(), anyString()))
            .thenReturn(messageQueueView);

        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);
    }

    @Test
    public void testAddTransactionSubscription() {
        this.clusterTransactionService.addTransactionSubscription(ctx, GROUP, TOPIC);

        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME, this.clusterTransactionService.getGroupClusterData().get(GROUP).getSet().stream().findAny().get().getCluster());
    }

    @Test
    public void testAddTransactionSubscriptionTopicList() {
        this.clusterTransactionService.addTransactionSubscription(ctx, GROUP, Lists.newArrayList(TOPIC + 1, TOPIC + 2));

        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME, this.clusterTransactionService.getGroupClusterData().get(GROUP).getSet().stream().findAny().get().getCluster());
    }

    @Test
    public void testReplaceTransactionSubscription() {
        this.clusterTransactionService.addTransactionSubscription(ctx, GROUP, TOPIC);

        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME, this.clusterTransactionService.getGroupClusterData().get(GROUP).getSet().stream().findAny().get().getCluster());

        this.brokerData.setCluster(CLUSTER_NAME + 1);
        this.clusterTransactionService.replaceTransactionSubscription(ctx, GROUP, Lists.newArrayList(TOPIC + 1));
        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());
        assertEquals(CLUSTER_NAME + 1, this.clusterTransactionService.getGroupClusterData().get(GROUP).getSet().stream().findAny().get().getCluster());
    }

    @Test
    public void testUnSubscribeAllTransactionTopic() {
        this.clusterTransactionService.addTransactionSubscription(ctx, GROUP, TOPIC);
        this.clusterTransactionService.unSubscribeAllTransactionTopic(ctx, GROUP);

        assertEquals(0, this.clusterTransactionService.getGroupClusterData().size());
    }

    @Test
    public void testScanProducerHeartBeat() throws Exception {
        when(this.producerManager.groupOnline(anyString())).thenReturn(true);

        Mockito.reset(this.topicRouteService);
        String brokerName2 = "broker-2-01";
        String clusterName2 = "broker-2";
        String brokerAddr2 = "127.0.0.2:10911";

        BrokerData brokerData = new BrokerData();
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName2);
        brokerData.setCluster(clusterName2);
        brokerData.setBrokerName(brokerName2);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, brokerName2);
        brokerData.setBrokerAddrs(brokerAddrs);
        topicRouteData.getQueueDatas().add(queueData);
        topicRouteData.getBrokerDatas().add(brokerData);
        when(this.topicRouteService.getAllMessageQueueView(any(), eq(TOPIC))).thenReturn(new MessageQueueView(TOPIC, topicRouteData, null));

        TopicRouteData clusterTopicRouteData = new TopicRouteData();
        QueueData clusterQueueData = new QueueData();
        BrokerData clusterBrokerData = new BrokerData();

        clusterQueueData.setBrokerName(BROKER_NAME);
        clusterTopicRouteData.setQueueDatas(Lists.newArrayList(clusterQueueData));
        clusterBrokerData.setCluster(CLUSTER_NAME);
        clusterBrokerData.setBrokerName(BROKER_NAME);
        brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, BROKER_ADDR);
        clusterBrokerData.setBrokerAddrs(brokerAddrs);
        clusterTopicRouteData.setBrokerDatas(Lists.newArrayList(clusterBrokerData));
        when(this.topicRouteService.getAllMessageQueueView(any(), eq(CLUSTER_NAME))).thenReturn(new MessageQueueView(CLUSTER_NAME, clusterTopicRouteData, null));

        TopicRouteData clusterTopicRouteData2 = new TopicRouteData();
        QueueData clusterQueueData2 = new QueueData();
        BrokerData clusterBrokerData2 = new BrokerData();

        clusterQueueData2.setBrokerName(brokerName2);
        clusterTopicRouteData2.setQueueDatas(Lists.newArrayList(clusterQueueData2));
        clusterBrokerData2.setCluster(clusterName2);
        clusterBrokerData2.setBrokerName(brokerName2);
        brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, brokerAddr2);
        clusterBrokerData2.setBrokerAddrs(brokerAddrs);
        clusterTopicRouteData2.setBrokerDatas(Lists.newArrayList(clusterBrokerData2));
        when(this.topicRouteService.getAllMessageQueueView(any(), eq(clusterName2))).thenReturn(new MessageQueueView(clusterName2, clusterTopicRouteData2, null));

        ConfigurationManager.getProxyConfig().setTransactionHeartbeatBatchNum(2);
        this.clusterTransactionService.start();
        Set<String> groupSet = new HashSet<>();

        for (int i = 0; i < 3; i++) {
            groupSet.add(GROUP + i);
            this.clusterTransactionService.addTransactionSubscription(ctx, GROUP + i, TOPIC);
        }

        ArgumentCaptor<String> brokerAddrArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HeartbeatData> heartbeatDataArgumentCaptor = ArgumentCaptor.forClass(HeartbeatData.class);
        when(mqClientAPIExt.sendHeartbeatOneway(
            brokerAddrArgumentCaptor.capture(),
            heartbeatDataArgumentCaptor.capture(),
            anyLong()
        )).thenReturn(CompletableFuture.completedFuture(null));

        this.clusterTransactionService.scanProducerHeartBeat();

        await().atMost(Duration.ofSeconds(1)).until(() -> brokerAddrArgumentCaptor.getAllValues().size() == 4);

        assertEquals(Lists.newArrayList(BROKER_ADDR, BROKER_ADDR, brokerAddr2, brokerAddr2),
            brokerAddrArgumentCaptor.getAllValues().stream().sorted().collect(Collectors.toList()));

        List<HeartbeatData> heartbeatDataList = heartbeatDataArgumentCaptor.getAllValues();

        for (final HeartbeatData heartbeatData : heartbeatDataList) {
            for (ProducerData producerData : heartbeatData.getProducerDataSet()) {
                groupSet.remove(producerData.getGroupName());
            }
        }

        assertTrue(groupSet.isEmpty());
        assertEquals(brokerName2, this.clusterTransactionService.getBrokerNameByAddr(brokerAddr2));
        assertEquals(BROKER_NAME, this.clusterTransactionService.getBrokerNameByAddr(BROKER_ADDR));

        // scan offline group
        when(this.producerManager.groupOnline(anyString())).thenReturn(false);
        ConfigurationManager.getProxyConfig().setTransactionGroupOfflineTimeoutMillis(100);
        this.clusterTransactionService.getGroupClusterData().clear();

        this.clusterTransactionService.addTransactionSubscription(ctx, GROUP, TOPIC);
        this.clusterTransactionService.scanProducerHeartBeat();
        assertEquals(1, this.clusterTransactionService.getGroupClusterData().size());

        TimeUnit.MILLISECONDS.sleep(200);
        this.clusterTransactionService.scanProducerHeartBeat();
        assertEquals(0, this.clusterTransactionService.getGroupClusterData().size());
    }
}
