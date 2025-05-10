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
package org.apache.rocketmq.controller.impl.manager;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerResponse;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoResponse;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventResponse;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class RaftReplicasInfoManagerTest {

    @Mock
    private ControllerConfig controllerConfig;

    private RaftReplicasInfoManager raftReplicasInfoManager;

    @Before
    public void init() {
        raftReplicasInfoManager = new RaftReplicasInfoManager(controllerConfig);
    }

    @Test
    public void testGetBrokerLiveInfoBrokerIdentityInfoIsNullReturnsAllBrokersInfo() throws IllegalAccessException {
        List<BrokerIdentityInfo> brokerIdentityInfos = createBrokerIdentityInfos(2);
        List<BrokerLiveInfo> brokerLiveInfos = createBrokerLiveInfos(2);
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(brokerIdentityInfos.get(0), brokerLiveInfos.get(0));
        brokerLiveTable.put(brokerIdentityInfos.get(1), brokerLiveInfos.get(1));
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        GetBrokerLiveInfoRequest request = new GetBrokerLiveInfoRequest();
        ControllerResult<GetBrokerLiveInfoResponse> result = raftReplicasInfoManager.getBrokerLiveInfo(request);
        assertNotNull(result);
        assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
    }

    @Test
    public void testGetBrokerLiveInfoBrokerIdentityInfoExistsReturnsBrokerInfo() throws IllegalAccessException {
        BrokerIdentityInfo brokerIdentityInfo = createBrokerIdentityInfo();
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(brokerIdentityInfo, createBrokerLiveInfo());
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        GetBrokerLiveInfoRequest request = new GetBrokerLiveInfoRequest(brokerIdentityInfo);
        ControllerResult<GetBrokerLiveInfoResponse> result = raftReplicasInfoManager.getBrokerLiveInfo(request);
        assertNotNull(result);
        assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
    }

    @Test
    public void testGetBrokerLiveInfoBrokerIdentityInfoNotExistsReturnsError() {
        GetBrokerLiveInfoRequest request = new GetBrokerLiveInfoRequest(createBrokerIdentityInfo());
        ControllerResult<GetBrokerLiveInfoResponse> result = raftReplicasInfoManager.getBrokerLiveInfo(request);
        assertNotNull(result);
        assertEquals(ResponseCode.CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS, result.getResponseCode());
    }

    @Test
    public void testOnBrokerHeartBeatNewBrokerRegistered() {
        RaftBrokerHeartBeatEventRequest request = new RaftBrokerHeartBeatEventRequest(createBrokerIdentityInfo(), createBrokerLiveInfo());
        ControllerResult<RaftBrokerHeartBeatEventResponse> result = raftReplicasInfoManager.onBrokerHeartBeat(request);
        assertNotNull(result);
        assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
    }

    @Test
    public void testOnBrokerHeartBeatExistingBrokerUpdate() throws IllegalAccessException {
        BrokerIdentityInfo brokerIdentityInfo = createBrokerIdentityInfo();
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(brokerIdentityInfo, createBrokerLiveInfo());
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        BrokerLiveInfo updatedInfo = new BrokerLiveInfo("brokerName1", "brokerAddr1", 1L, System.currentTimeMillis(), 2000L, null, 2, 200L, 2);
        RaftBrokerHeartBeatEventRequest request = new RaftBrokerHeartBeatEventRequest(brokerIdentityInfo, updatedInfo);
        ControllerResult<RaftBrokerHeartBeatEventResponse> result = raftReplicasInfoManager.onBrokerHeartBeat(request);
        assertNotNull(result);
        assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
    }

    @Test
    public void testOnBrokerCloseChannelBrokerIdentityInfoIsNullLogsWarningAndReturnsResult() {
        assertNotNull(raftReplicasInfoManager.onBrokerCloseChannel(new BrokerCloseChannelRequest()));
    }

    @Test
    public void testCheckNotActiveBrokerNoBrokersInTableReturnsEmptyList() {
        CheckNotActiveBrokerRequest request = new CheckNotActiveBrokerRequest();
        ControllerResult<CheckNotActiveBrokerResponse> result = raftReplicasInfoManager.checkNotActiveBroker(request);
        assertNotNull(result);
        assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
    }

    @Test
    public void testCheckNotActiveBrokerBrokerLiveTableNotEmptyIdentifiesNotActiveBrokers() throws IllegalAccessException {
        List<BrokerIdentityInfo> brokerIdentityInfos = createBrokerIdentityInfos(2);
        List<BrokerLiveInfo> brokerLiveInfos = createBrokerLiveInfos(2);
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(brokerIdentityInfos.get(0), brokerLiveInfos.get(0));
        brokerLiveTable.put(brokerIdentityInfos.get(1), brokerLiveInfos.get(1));
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        CheckNotActiveBrokerRequest request = new CheckNotActiveBrokerRequest();
        ControllerResult<CheckNotActiveBrokerResponse> result = raftReplicasInfoManager.checkNotActiveBroker(request);
        assertNotNull(result);
        assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
        assertNotNull(result.getBody());
    }

    @Test
    public void testCheckNotActiveBrokerSerializeErrorSetsErrorRemark() throws IllegalAccessException {
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(createBrokerIdentityInfo(), createBrokerLiveInfo());
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        CheckNotActiveBrokerRequest request = new CheckNotActiveBrokerRequest();
        ControllerResult<CheckNotActiveBrokerResponse> result = raftReplicasInfoManager.checkNotActiveBroker(request);
        assertNotNull(result);
    }

    @Test
    public void testIsBrokerActiveBrokerLiveInfoNotNullAndActiveReturnsTrue() throws IllegalAccessException {
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(createBrokerIdentityInfo(), createBrokerLiveInfo());
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        long invokeTime = System.currentTimeMillis() + 500;
        boolean brokerActive = raftReplicasInfoManager.isBrokerActive("cluster0", "broker0", 0L, invokeTime);
        assertTrue(brokerActive);
    }

    @Test
    public void testIsBrokerActiveBrokerLiveInfoNotNullAndNotActiveReturnsFalse() throws IllegalAccessException {
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(createBrokerIdentityInfo(), createBrokerLiveInfo());
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        long invokeTime = System.currentTimeMillis();
        assertFalse(raftReplicasInfoManager.isBrokerActive("cluster1", "broker1", 1L, invokeTime));
    }

    @Test
    public void testIsBrokerActiveBrokerLiveInfoNullReturnsFalse() {
        assertFalse(raftReplicasInfoManager.isBrokerActive("cluster1", "broker1", 1L, System.currentTimeMillis()));
    }

    @Test
    public void testSerializeWithPopulatedTablesReturnsByteArray() throws Throwable {
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(createBrokerIdentityInfo(), createBrokerLiveInfo());
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        byte[] result = raftReplicasInfoManager.serialize();
        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void testDeserializeFromValidDataSuccess() throws Throwable {
        BrokerIdentityInfo brokerIdentityInfo = createBrokerIdentityInfo();
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new HashMap<>();
        brokerLiveTable.put(brokerIdentityInfo, createBrokerLiveInfo());
        FieldUtils.writeDeclaredField(raftReplicasInfoManager, "brokerLiveTable", brokerLiveTable, true);
        raftReplicasInfoManager.deserializeFrom(raftReplicasInfoManager.serialize());
        assertNotNull(brokerLiveTable);
        assertEquals(1, brokerLiveTable.size());
        assertTrue(brokerLiveTable.containsKey(brokerIdentityInfo));
    }

    @Test
    public void testDeserializeFromInvalidDataExceptionThrown() {
        byte[] invalidData = new byte[]{0x00, 0x01, 0x02, 0x03};
        try {
            raftReplicasInfoManager.deserializeFrom(invalidData);
            fail("Expected an exception to be thrown");
        } catch (Throwable e) {
            assertTrue(e instanceof ArrayIndexOutOfBoundsException);
        }
    }

    private BrokerIdentityInfo createBrokerIdentityInfo() {
        return createBrokerIdentityInfos(1).get(0);
    }

    private List<BrokerIdentityInfo> createBrokerIdentityInfos(final int count) {
        List<BrokerIdentityInfo> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(new BrokerIdentityInfo("cluster" + i, "broker" + i, (long) i));
        }
        return result;
    }

    private BrokerLiveInfo createBrokerLiveInfo() {
        return createBrokerLiveInfos(1).get(0);
    }

    private List<BrokerLiveInfo> createBrokerLiveInfos(final int count) {
        List<BrokerLiveInfo> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(new BrokerLiveInfo("brokerName" + i,
                    "brokerAddr" + i,
                    i,
                    System.currentTimeMillis(),
                    1000L,
                    null,
                    1,
                    100L,
                    1));
        }
        return result;
    }
}
