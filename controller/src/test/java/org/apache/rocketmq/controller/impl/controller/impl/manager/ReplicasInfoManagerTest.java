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
package org.apache.rocketmq.controller.impl.controller.impl.manager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.controller.impl.manager.ReplicasInfoManager;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.ElectMasterEvent;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ReplicasInfoManagerTest {
    private ReplicasInfoManager replicasInfoManager;

    @Before
    public void init() {
        final ControllerConfig config = new ControllerConfig();
        config.setEnableElectUncleanMaster(false);
        this.replicasInfoManager = new ReplicasInfoManager(config);
    }

    public boolean registerNewBroker(String clusterName, String brokerName, String brokerAddress,
        boolean isFirstRegisteredBroker) {
        // Register new broker
        final RegisterBrokerToControllerRequestHeader registerRequest =
            new RegisterBrokerToControllerRequestHeader(clusterName, brokerName, brokerAddress);
        final ControllerResult<RegisterBrokerToControllerResponseHeader> registerResult = this.replicasInfoManager.registerBroker(registerRequest);
        apply(registerResult.getEvents());

        if (isFirstRegisteredBroker) {
            final ControllerResult<GetReplicaInfoResponseHeader> getInfoResult = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName));
            final GetReplicaInfoResponseHeader replicaInfo = getInfoResult.getResponse();
            assertEquals(replicaInfo.getMasterAddress(), brokerAddress);
            assertEquals(replicaInfo.getMasterEpoch(), 1);
        } else {
            final RegisterBrokerToControllerResponseHeader response = registerResult.getResponse();
            assertTrue(response.getBrokerId() > 0);
        }
        return true;
    }

    private boolean alterNewInSyncSet(String brokerName, String masterAddress, int masterEpoch,
        Set<String> newSyncStateSet, int syncStateSetEpoch) {
        final AlterSyncStateSetRequestHeader alterRequest =
            new AlterSyncStateSetRequestHeader(brokerName, masterAddress, masterEpoch);
        final ControllerResult<AlterSyncStateSetResponseHeader> result = this.replicasInfoManager.alterSyncStateSet(alterRequest, new SyncStateSet(newSyncStateSet, syncStateSetEpoch), (va1, va2) -> true);
        apply(result.getEvents());

        final ControllerResult<GetReplicaInfoResponseHeader> resp = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName));
        final GetReplicaInfoResponseHeader replicaInfo = resp.getResponse();
        final SyncStateSet syncStateSet = RemotingSerializable.decode(resp.getBody(), SyncStateSet.class);

        assertArrayEquals(syncStateSet.getSyncStateSet().toArray(), newSyncStateSet.toArray());
        assertEquals(syncStateSet.getSyncStateSetEpoch(), syncStateSetEpoch + 1);
        return true;
    }

    private void apply(final List<EventMessage> events) {
        for (EventMessage event : events) {
            this.replicasInfoManager.applyEvent(event);
        }
    }

    public void mockMetaData() {
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9000", true);
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9001", false);
        registerNewBroker("cluster1", "broker1", "127.0.0.1:9002", false);
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        newSyncStateSet.add("127.0.0.1:9001");
        newSyncStateSet.add("127.0.0.1:9002");
        assertTrue(alterNewInSyncSet("broker1", "127.0.0.1:9000", 1, newSyncStateSet, 1));
    }

    @Test
    public void testElectMaster() {
        mockMetaData();
        final ElectMasterRequestHeader request = new ElectMasterRequestHeader("broker1");
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(request, (clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"));
        final ElectMasterResponseHeader response = cResult.getResponse();
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertNotEquals(response.getNewMasterAddress(), "127.0.0.1:9000");

        final Set<String> brokerSet = new HashSet<>();
        brokerSet.add("127.0.0.1:9000");
        brokerSet.add("127.0.0.1:9001");
        brokerSet.add("127.0.0.1:9002");
        final ElectMasterRequestHeader assignRequest = new ElectMasterRequestHeader("cluster1","broker1", "127.0.0.1:9000");
        final ControllerResult<ElectMasterResponseHeader> cResult1 = this.replicasInfoManager.electMaster(assignRequest, (clusterName, brokerAddress) -> brokerAddress.contains("127.0.0.1:9000"));
        assertEquals( cResult1.getResponseCode(), ResponseCode.CONTROLLER_INVALID_REQUEST);


        final ElectMasterRequestHeader assignRequest1 = new ElectMasterRequestHeader("cluster1","broker1", "127.0.0.1:9001");
        final ControllerResult<ElectMasterResponseHeader> cResult2 = this.replicasInfoManager.electMaster(assignRequest1, (clusterName, brokerAddress) -> brokerAddress.equals("127.0.0.1:9000"));
        assertEquals( cResult2.getResponseCode(), ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE);

        final ElectMasterRequestHeader assignRequest2 = new ElectMasterRequestHeader("cluster1","broker1", "127.0.0.1:9001");
        final ControllerResult<ElectMasterResponseHeader> cResult3 = this.replicasInfoManager.electMaster(assignRequest2, (clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"));
        assertEquals( cResult3.getResponseCode(), ResponseCode.SUCCESS);
        final ElectMasterResponseHeader response3 = cResult3.getResponse();
        assertEquals(response3.getNewMasterAddress(),"127.0.0.1:9001");
        assertEquals(response.getMasterEpoch(), 2);
        assertFalse(response.getNewMasterAddress().isEmpty());
        assertNotEquals(response.getNewMasterAddress(), "127.0.0.1:9000");

    }

    @Test
    public void testAllReplicasShutdownAndRestart() {
        mockMetaData();
        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add("127.0.0.1:9000");
        assertTrue(alterNewInSyncSet("broker1", "127.0.0.1:9000", 1, newSyncStateSet, 2));

        // Now we trigger electMaster api, which means the old master is shutdown and want to elect a new master.
        // However, the syncStateSet in statemachine is {"127.0.0.1:9000"}, not more replicas can be elected as master, it will be failed.
        final ElectMasterRequestHeader electRequest = new ElectMasterRequestHeader("broker1");
        final ControllerResult<ElectMasterResponseHeader> cResult = this.replicasInfoManager.electMaster(electRequest, (clusterName, brokerAddress) -> !brokerAddress.equals("127.0.0.1:9000"));
        final List<EventMessage> events = cResult.getEvents();
        assertEquals(events.size(), 1);
        final ElectMasterEvent event = (ElectMasterEvent) events.get(0);
        assertFalse(event.getNewMasterElected());

        apply(cResult.getEvents());

        final GetReplicaInfoResponseHeader replicaInfo = this.replicasInfoManager.getReplicaInfo(new GetReplicaInfoRequestHeader("broker1")).getResponse();
        assertEquals(replicaInfo.getMasterAddress(), "");
        assertEquals(replicaInfo.getMasterEpoch(), 2);
    }

}