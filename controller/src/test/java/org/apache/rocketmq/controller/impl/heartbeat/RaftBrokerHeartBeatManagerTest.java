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
package org.apache.rocketmq.controller.impl.heartbeat;

import com.alibaba.fastjson2.JSON;
import io.netty.channel.Channel;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.controller.impl.JRaftController;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoResponse;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RaftBrokerHeartBeatManagerTest {

    @Mock
    private JRaftController controller;

    private RaftBrokerHeartBeatManager raftBrokerHeartBeatManager;

    @Before
    public void init() throws IllegalAccessException {
        ControllerConfig controllerConfig = new ControllerConfig();
        raftBrokerHeartBeatManager = new RaftBrokerHeartBeatManager(controllerConfig);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "controller", controller, true);
    }

    @Test
    public void testOnBrokerHeartbeatSuccess() {
        Channel channel = mock(Channel.class);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        when(controller.onBrokerHeartBeat(any())).thenReturn(future);
        raftBrokerHeartBeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:10911", 1L, 3000L, channel, 1, 1000L, 500L, 1);
        verify(channel, never()).close();
    }

    @Test
    public void testOnBrokerHeartbeatLeaderNotAvailable() {
        Channel channel = mock(Channel.class);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_NOT_LEADER, "Not Leader"));
        when(controller.onBrokerHeartBeat(any())).thenReturn(future);
        raftBrokerHeartBeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:10911", 1L, 3000L, channel, 1, 1000L, 500L, 1);
        verify(channel, never()).close();
    }

    @Test
    public void testOnBrokerHeartbeatException() throws Exception {
        Channel channel = mock(Channel.class);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        when(controller.onBrokerHeartBeat(any())).thenReturn(future);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "brokerChannelIdentityInfoMap", null, true);
        assertThrows(NullPointerException.class, () -> raftBrokerHeartBeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:10911", 1L, 3000L, channel, 1, 1000L, 500L, 1));
    }

    @Test
    public void testOnBrokerChannelCloseBrokerIdentityInfoNotNullSuccess() throws Exception {
        Channel channel = mock(Channel.class);
        BrokerIdentityInfo brokerIdentityInfo = new BrokerIdentityInfo("cluster1", "broker1", 1L);
        Map<Channel, BrokerIdentityInfo> brokerChannelIdentityInfoMap = new HashMap<>();
        brokerChannelIdentityInfoMap.put(channel, brokerIdentityInfo);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "brokerChannelIdentityInfoMap", brokerChannelIdentityInfoMap, true);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        when(controller.onBrokerCloseChannel(any(BrokerCloseChannelRequest.class))).thenReturn(future);
        raftBrokerHeartBeatManager.onBrokerChannelClose(channel);
        verify(controller).onBrokerCloseChannel(any(BrokerCloseChannelRequest.class));
    }

    @Test
    public void testOnBrokerChannelCloseBrokerIdentityInfoNotNullException() throws Exception {
        Channel channel = mock(Channel.class);
        BrokerIdentityInfo brokerIdentityInfo = new BrokerIdentityInfo("cluster1", "broker1", 1L);
        Map<Channel, BrokerIdentityInfo> brokerChannelIdentityInfoMap = new HashMap<>();
        brokerChannelIdentityInfoMap.put(channel, brokerIdentityInfo);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "brokerChannelIdentityInfoMap", brokerChannelIdentityInfoMap, true);
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new ExecutionException(new RuntimeException("Test Exception")));
        when(controller.onBrokerCloseChannel(any(BrokerCloseChannelRequest.class))).thenReturn(future);
        raftBrokerHeartBeatManager.onBrokerChannelClose(channel);
        verify(controller).onBrokerCloseChannel(any(BrokerCloseChannelRequest.class));
    }

    @Test
    public void testOnBrokerChannelCloseBrokerIdentityInfoNull() {
        Channel channel = mock(Channel.class);
        raftBrokerHeartBeatManager.onBrokerChannelClose(channel);
        verify(controller, never()).onBrokerCloseChannel(any(BrokerCloseChannelRequest.class));
    }

    @Test
    public void testOnBrokerChannelCloseBrokerIdentityInfoNotNullTimeoutException() throws Exception {
        Channel channel = mock(Channel.class);
        BrokerIdentityInfo brokerIdentityInfo = new BrokerIdentityInfo("cluster1", "broker1", 1L);
        Map<Channel, BrokerIdentityInfo> brokerChannelIdentityInfoMap = new HashMap<>();
        brokerChannelIdentityInfoMap.put(channel, brokerIdentityInfo);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "brokerChannelIdentityInfoMap", brokerChannelIdentityInfoMap, true);
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        when(controller.onBrokerCloseChannel(any(BrokerCloseChannelRequest.class))).thenReturn(future);
        raftBrokerHeartBeatManager.onBrokerChannelClose(channel);
        verify(controller).onBrokerCloseChannel(any(BrokerCloseChannelRequest.class));
    }

    @Test
    public void testScanNotActiveBrokerSuccess() throws Exception {
        when(controller.isLeaderState()).thenReturn(true);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "firstReceivedHeartbeatTime", System.currentTimeMillis() + 1000L, true);

        ControllerConfig controllerConfig = mock(ControllerConfig.class);
        JraftConfig jraftConfig = mock(JraftConfig.class);
        when(jraftConfig.getjRaftScanWaitTimeoutMs()).thenReturn(10000);
        when(controllerConfig.getJraftConfig()).thenReturn(jraftConfig);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "controllerConfig", controllerConfig, true);

        List<BrokerIdentityInfo> inactiveBrokers = new ArrayList<>();
        BrokerIdentityInfo brokerInfo = new BrokerIdentityInfo("testCluster", "testBroker", 1L);
        inactiveBrokers.add(brokerInfo);

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success");
        response.setBody(JSON.toJSONString(inactiveBrokers).getBytes());
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(response);
        when(controller.checkNotActiveBroker(any())).thenReturn(future);

        Channel channel = mock(Channel.class);
        Map<Channel, BrokerIdentityInfo> brokerChannelMap = new HashMap<>();
        brokerChannelMap.put(channel, brokerInfo);
        FieldUtils.writeDeclaredField(raftBrokerHeartBeatManager, "brokerChannelIdentityInfoMap", brokerChannelMap, true);

        Method method = RaftBrokerHeartBeatManager.class.getDeclaredMethod("scanNotActiveBroker");
        method.setAccessible(true);
        method.invoke(raftBrokerHeartBeatManager);

        verify(controller).checkNotActiveBroker(any(CheckNotActiveBrokerRequest.class));
    }

    @Test
    public void testGetBrokerLiveInfoSuccess() throws Exception {
        String clusterName = "cluster1";
        String brokerName = "broker1";
        Long brokerId = 1L;
        BrokerIdentityInfo brokerIdentityInfo = new BrokerIdentityInfo(clusterName, brokerName, brokerId);
        BrokerLiveInfo expectedBrokerLiveInfo = new BrokerLiveInfo(brokerName, "127.0.0.1:10911", brokerId, System.currentTimeMillis(), 3000L, null, 1, 1000L, 500);
        Map<BrokerIdentityInfo, BrokerLiveInfo> expectedResponse = new HashMap<>();
        expectedResponse.put(brokerIdentityInfo, expectedBrokerLiveInfo);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(expectedResponse).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        BrokerLiveInfo brokerLiveInfo = raftBrokerHeartBeatManager.getBrokerLiveInfo(clusterName, brokerName, brokerId);
        assertEquals(expectedBrokerLiveInfo.getBrokerName(), brokerLiveInfo.getBrokerName());
        assertEquals(expectedBrokerLiveInfo.getBrokerAddr(), brokerLiveInfo.getBrokerAddr());
        assertEquals(expectedBrokerLiveInfo.getBrokerId(), brokerLiveInfo.getBrokerId());
    }

    @Test
    public void testGetBrokerLiveInfoAllBrokers() throws Exception {
        String clusterName = "cluster1";
        String brokerName = "broker1";
        Long brokerId = 1L;
        BrokerIdentityInfo brokerIdentityInfo = new BrokerIdentityInfo(clusterName, brokerName, brokerId);
        BrokerLiveInfo expectedBrokerLiveInfo = new BrokerLiveInfo(brokerName, "127.0.0.1:10911", brokerId, System.currentTimeMillis(), 3000L, null, 1, 1000L, 500);
        Map<BrokerIdentityInfo, BrokerLiveInfo> expectedResponse = new HashMap<>();
        expectedResponse.put(brokerIdentityInfo, expectedBrokerLiveInfo);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(expectedResponse).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        BrokerLiveInfo brokerLiveInfo = raftBrokerHeartBeatManager.getBrokerLiveInfo(null, null, null);
        assertNull(brokerLiveInfo);
    }

    @Test
    public void testIsBrokerActiveBrokerActive() throws Exception {
        String clusterName = "cluster1";
        String brokerName = "broker1";
        Long brokerId = 1L;
        BrokerLiveInfo brokerLiveInfo = new BrokerLiveInfo(brokerName, "127.0.0.1:10911", brokerId, System.currentTimeMillis(), 3000L, null, 1, 1000L, 500);
        Map<BrokerIdentityInfo, BrokerLiveInfo> responseMap = new HashMap<>();
        responseMap.put(new BrokerIdentityInfo(clusterName, brokerName, brokerId), brokerLiveInfo);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(responseMap).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        assertTrue(raftBrokerHeartBeatManager.isBrokerActive(clusterName, brokerName, brokerId));
    }

    @Test
    public void testIsBrokerActiveBrokerNotActive() throws Exception {
        String clusterName = "cluster1";
        String brokerName = "broker1";
        Long brokerId = 1L;
        BrokerLiveInfo brokerLiveInfo = new BrokerLiveInfo(brokerName, "127.0.0.1:10911", brokerId, System.currentTimeMillis() - 4000L, 3000L, null, 1, 1000L, 500);
        Map<BrokerIdentityInfo, BrokerLiveInfo> responseMap = new HashMap<>();
        responseMap.put(new BrokerIdentityInfo(clusterName, brokerName, brokerId), brokerLiveInfo);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(responseMap).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        assertFalse(raftBrokerHeartBeatManager.isBrokerActive(clusterName, brokerName, brokerId));
    }

    @Test
    public void testIsBrokerActiveException() {
        String clusterName = "cluster1";
        String brokerName = "broker1";
        Long brokerId = 1L;
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new ExecutionException(new RuntimeException("Test Exception")));
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        assertFalse(raftBrokerHeartBeatManager.isBrokerActive(clusterName, brokerName, brokerId));
    }

    @Test
    public void testIsBrokerActiveNoInfo() throws Exception {
        String clusterName = "cluster1";
        String brokerName = "broker1";
        Long brokerId = 1L;
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(new HashMap<BrokerIdentityInfo, BrokerLiveInfo>()).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        assertFalse(raftBrokerHeartBeatManager.isBrokerActive(clusterName, brokerName, brokerId));
    }

    @Test
    public void testIsBrokerActiveInvalidResponseCode() {
        String clusterName = "cluster1";
        String brokerName = "broker1";
        Long brokerId = 1L;
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.RPC_TIME_OUT, "Timeout"));
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        assertFalse(raftBrokerHeartBeatManager.isBrokerActive(clusterName, brokerName, brokerId));
    }

    @Test
    public void testGetActiveBrokersNumAllBrokers() throws Exception {
        String clusterName1 = "cluster1";
        String brokerName1 = "broker1";
        Long brokerId1 = 1L;
        String clusterName2 = "cluster2";
        String brokerName2 = "broker2";
        Long brokerId2 = 2L;
        BrokerIdentityInfo brokerIdentityInfo1 = new BrokerIdentityInfo(clusterName1, brokerName1, brokerId1);
        BrokerIdentityInfo brokerIdentityInfo2 = new BrokerIdentityInfo(clusterName2, brokerName2, brokerId2);
        BrokerLiveInfo brokerLiveInfo1 = new BrokerLiveInfo(brokerName1, "127.0.0.1:10911", brokerId1, System.currentTimeMillis(), 3000L, null, 1, 1000L, 500);
        BrokerLiveInfo brokerLiveInfo2 = new BrokerLiveInfo(brokerName2, "127.0.0.1:10912", brokerId2, System.currentTimeMillis(), 3000L, null, 1, 1000L, 500);
        Map<BrokerIdentityInfo, BrokerLiveInfo> responseMap = new HashMap<>();
        responseMap.put(brokerIdentityInfo1, brokerLiveInfo1);
        responseMap.put(brokerIdentityInfo2, brokerLiveInfo2);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(responseMap).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        Map<String, Map<String, Integer>> activeBrokersNum = raftBrokerHeartBeatManager.getActiveBrokersNum();
        assertEquals(2, activeBrokersNum.size());
        assertEquals(1, activeBrokersNum.get(clusterName1).size());
        assertEquals(1, activeBrokersNum.get(clusterName2).size());
        assertEquals(1, (int) activeBrokersNum.get(clusterName1).get(brokerName1));
        assertEquals(1, (int) activeBrokersNum.get(clusterName2).get(brokerName2));
    }

    @Test
    public void testGetActiveBrokersNum() throws Exception {
        String clusterName1 = "cluster1";
        String brokerName1 = "broker1";
        Long brokerId1 = 1L;
        String clusterName2 = "cluster2";
        String brokerName2 = "broker2";
        Long brokerId2 = 2L;
        BrokerIdentityInfo brokerIdentityInfo1 = new BrokerIdentityInfo(clusterName1, brokerName1, brokerId1);
        BrokerIdentityInfo brokerIdentityInfo2 = new BrokerIdentityInfo(clusterName2, brokerName2, brokerId2);
        BrokerLiveInfo brokerLiveInfo1 = new BrokerLiveInfo(brokerName1, "127.0.0.1:10911", brokerId1, System.currentTimeMillis(), 3000L, null, 1, 1000L, 500);
        BrokerLiveInfo brokerLiveInfo2 = new BrokerLiveInfo(brokerName2, "127.0.0.1:10912", brokerId2, System.currentTimeMillis() - 4000L, 3000L, null, 1, 1000L, 500);
        Map<BrokerIdentityInfo, BrokerLiveInfo> responseMap = new HashMap<>();
        responseMap.put(brokerIdentityInfo1, brokerLiveInfo1);
        responseMap.put(brokerIdentityInfo2, brokerLiveInfo2);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(responseMap).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        Map<String, Map<String, Integer>> activeBrokersNum = raftBrokerHeartBeatManager.getActiveBrokersNum();
        assertEquals(1, activeBrokersNum.size());
    }

    @Test
    public void testGetActiveBrokersNumException() {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new ExecutionException(new RuntimeException("Test Exception")));
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        Map<String, Map<String, Integer>> activeBrokersNum = raftBrokerHeartBeatManager.getActiveBrokersNum();
        assertTrue(activeBrokersNum.isEmpty());
    }

    @Test
    public void testGetActiveBrokersNumNoBrokers() throws Exception {
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "Success"));
        future.get().writeCustomHeader(new GetBrokerLiveInfoResponse());
        future.get().setBody(JSON.toJSONString(new HashMap<BrokerIdentityInfo, BrokerLiveInfo>()).getBytes());
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        Map<String, Map<String, Integer>> activeBrokersNum = raftBrokerHeartBeatManager.getActiveBrokersNum();
        assertTrue(activeBrokersNum.isEmpty());
    }

    @Test
    public void testGetActiveBrokersNumInvalidResponseCode() {
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.RPC_TIME_OUT, "Timeout"));
        when(controller.getBrokerLiveInfo(any())).thenReturn(future);
        Map<String, Map<String, Integer>> activeBrokersNum = raftBrokerHeartBeatManager.getActiveBrokersNum();
        assertTrue(activeBrokersNum.isEmpty());
    }
}
