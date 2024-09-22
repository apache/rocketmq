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
package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.rpc.RpcClient;
import org.apache.rocketmq.remoting.rpc.RpcRequest;
import org.apache.rocketmq.remoting.rpc.RpcResponse;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicQueueMappingCleanServiceTest {
    
    @Mock
    private BrokerController brokerController;
    
    @Mock
    private TopicQueueMappingManager topicQueueMappingManager;
    
    @Mock
    private RpcClient rpcClient;
    
    @Mock
    private MessageStoreConfig messageStoreConfig;
    
    @Mock
    private BrokerConfig brokerConfig;
    
    @Mock
    private BrokerOuterAPI brokerOuterAPI;
    
    private TopicQueueMappingCleanService topicQueueMappingCleanService;
    
    private final String defaultTopic = "defaultTopic";
    
    private final String defaultBroker = "defaultBroker";
    
    private final String deleteWhen = "00;01;02;03;04;05;06;07;08;09;10;11;12;13;14;15;16;17;18;19;20;21;22;23";
    
    @Before
    public void init() {
        when(brokerOuterAPI.getRpcClient()).thenReturn(rpcClient);
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerController.getTopicQueueMappingManager()).thenReturn(topicQueueMappingManager);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        topicQueueMappingCleanService = new TopicQueueMappingCleanService(brokerController);
    }
    
    @Test
    public void testCleanItemExpiredNoChange() throws Exception {
        when(messageStoreConfig.getDeleteWhen()).thenReturn("04");
        topicQueueMappingCleanService.cleanItemExpired();
        verify(topicQueueMappingManager, never()).updateTopicQueueMapping(any(), anyBoolean(), anyBoolean(), anyBoolean());
    }
    
    @Test
    public void testCleanItemExpiredWithChange() throws Exception {
        when(messageStoreConfig.getDeleteWhen()).thenReturn(deleteWhen);
        TopicQueueMappingDetail mappingDetail = new TopicQueueMappingDetail(defaultTopic, 2, defaultBroker, 1);
        mappingDetail.getHostedQueues().put(0,
                Arrays.asList(new LogicQueueMappingItem(0, 0, defaultBroker, 0, 0, 100, 0, 0),
                        new LogicQueueMappingItem(0, 1, defaultBroker, 1, 100, 200, 0, 0)));
        when(topicQueueMappingManager.getTopicQueueMappingTable()).thenReturn(new ConcurrentHashMap<>(Collections.singletonMap(defaultTopic, mappingDetail)));
        when(brokerConfig.getBrokerName()).thenReturn(defaultBroker);
        TopicStatsTable topicStatsTable = mock(TopicStatsTable.class);
        Map<MessageQueue, TopicOffset> offsetTable = new ConcurrentHashMap<>();
        TopicOffset topicOffset = new TopicOffset();
        topicOffset.setMinOffset(0);
        topicOffset.setMaxOffset(0);
        MessageQueue messageQueue = new MessageQueue(defaultTopic, defaultBroker, 0);
        offsetTable.put(messageQueue, topicOffset);
        when(topicStatsTable.getOffsetTable()).thenReturn(offsetTable);
        when(rpcClient.invoke(any(RpcRequest.class), anyLong())).thenReturn(CompletableFuture.completedFuture(new RpcResponse(0, null, topicStatsTable)));
        DataVersion dataVersion = mock(DataVersion.class);
        when(topicQueueMappingManager.getDataVersion()).thenReturn(dataVersion);
        topicQueueMappingCleanService.cleanItemExpired();
        verify(topicQueueMappingManager, times(1)).updateTopicQueueMapping(any(), anyBoolean(), anyBoolean(), anyBoolean());
    }
    
    @Test
    public void testCleanItemListMoreThanSecondGen() throws Exception {
        when(brokerConfig.getBrokerName()).thenReturn(defaultBroker);
        when(messageStoreConfig.getDeleteWhen()).thenReturn(deleteWhen);
        TopicQueueMappingDetail mappingDetail = new TopicQueueMappingDetail(defaultTopic, 1, defaultBroker, 1);
        mappingDetail.setHostedQueues(new ConcurrentHashMap<>());
        LogicQueueMappingItem logicQueueMappingItem = mock(LogicQueueMappingItem.class);
        when(logicQueueMappingItem.getBname()).thenReturn("broker");
        mappingDetail.getHostedQueues().put(0, Collections.singletonList(logicQueueMappingItem));
        ConcurrentMap<String, TopicQueueMappingDetail> topicQueueMappingTable = new ConcurrentHashMap<>();
        topicQueueMappingTable.put(defaultBroker, mappingDetail);
        when(topicQueueMappingManager.getTopicQueueMappingTable()).thenReturn(topicQueueMappingTable);
        TopicRouteData topicRouteData = new TopicRouteData();
        when(brokerOuterAPI.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(topicRouteData);
        topicQueueMappingCleanService.cleanItemListMoreThanSecondGen();
        verify(brokerOuterAPI, times(1)).getTopicRouteInfoFromNameServer(any(), anyLong());
    }
    
    @Test
    public void testCleanItemListMoreThanSecondGenNoChange() throws Exception {
        when(messageStoreConfig.getDeleteWhen()).thenReturn("04");
        TopicQueueMappingDetail mappingDetail = new TopicQueueMappingDetail(defaultTopic, 1, defaultBroker, 1);
        mappingDetail.setHostedQueues(new ConcurrentHashMap<>());
        topicQueueMappingCleanService.cleanItemListMoreThanSecondGen();
        verify(brokerOuterAPI, never()).getTopicRouteInfoFromNameServer(anyString(), anyLong());
        verify(rpcClient, never()).invoke(any(RpcRequest.class), anyLong());
    }
    
    @Test
    public void testCleanItemListMoreThanSecondGenException() throws Exception {
        when(brokerConfig.getBrokerName()).thenReturn(defaultBroker);
        when(messageStoreConfig.getDeleteWhen()).thenReturn(deleteWhen);
        TopicQueueMappingDetail mappingDetail = new TopicQueueMappingDetail(defaultTopic, 1, defaultBroker, 1);
        mappingDetail.setHostedQueues(new ConcurrentHashMap<>());
        LogicQueueMappingItem logicQueueMappingItem = mock(LogicQueueMappingItem.class);
        when(logicQueueMappingItem.getBname()).thenReturn("broker");
        mappingDetail.getHostedQueues().put(0, Collections.singletonList(logicQueueMappingItem));
        ConcurrentMap<String, TopicQueueMappingDetail> topicQueueMappingTable = new ConcurrentHashMap<>();
        topicQueueMappingTable.put(defaultBroker, mappingDetail);
        when(topicQueueMappingManager.getTopicQueueMappingTable()).thenReturn(topicQueueMappingTable);
        when(brokerOuterAPI.getTopicRouteInfoFromNameServer(any(), anyLong())).thenThrow(new RemotingException("Test exception"));
        topicQueueMappingCleanService.cleanItemListMoreThanSecondGen();
        verify(brokerOuterAPI, times(1)).getTopicRouteInfoFromNameServer(any(), anyLong());
    }
}
