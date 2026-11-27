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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.helper.BrokerValidPredicate;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.event.EventSerializer;
import org.apache.rocketmq.controller.impl.event.ListEventSerializer;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class BrokerStartupRegistrationTest {
    private static final String CLUSTER = "startup-cluster";
    private static final String BROKER = "startup-broker";
    private static final String MASTER_ADDRESS = "127.0.0.1:10911";
    private static final String SLAVE_ADDRESS = "127.0.0.1:20911";
    private static final BrokerValidPredicate ALL_ALIVE = (cluster, broker, id) -> true;
    private static final BrokerValidPredicate NONE_ALIVE = (cluster, broker, id) -> false;

    private final boolean raft;
    private final EventSerializer serializer = new EventSerializer();
    private ReplicasInfoManager manager;

    @Parameterized.Parameters(name = "raft={0}")
    public static Collection<Object[]> controllerTypes() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    public BrokerStartupRegistrationTest(boolean raft) {
        this.raft = raft;
    }

    @Before
    public void setUp() {
        manager = newManager();
        apply(manager.applyBrokerId(new ApplyBrokerIdRequestHeader(CLUSTER, BROKER, 1L, MASTER_ADDRESS + ";1")));
        apply(manager.applyBrokerId(new ApplyBrokerIdRequestHeader(CLUSTER, BROKER, 2L, SLAVE_ADDRESS + ";1")));
        apply(register(1L, MASTER_ADDRESS, 1L, "first-master", ALL_ALIVE));
        apply(register(2L, SLAVE_ADDRESS, 1L, "first-slave", ALL_ALIVE));
        apply(manager.electMaster(ElectMasterRequestHeader.ofBrokerTrigger(CLUSTER, BROKER, 1L),
            new DefaultElectPolicy(ALL_ALIVE, null)));
        apply(manager.alterSyncStateSet(new AlterSyncStateSetRequestHeader(BROKER, 1L, 1),
            new SyncStateSet(new HashSet<>(Arrays.asList(1L, 2L)), 1), ALL_ALIVE));
    }

    @Test
    public void testMasterRestartAdvancesEpochWithoutShrinkingSyncStateSet() {
        SyncStateSet before = syncStateSet();
        ControllerResult<RegisterBrokerToControllerResponseHeader> result =
            register(1L, MASTER_ADDRESS, 2L, "second-master", NONE_ALIVE);

        assertEquals(ResponseCode.SUCCESS, result.getResponseCode());
        assertEquals(Long.valueOf(2L), result.getResponse().getBrokerStartupGeneration());
        assertEquals("second-master", result.getResponse().getBrokerStartupId());
        assertNull(result.getResponse().getMasterBrokerId());
        assertEquals(Integer.valueOf(2), result.getResponse().getMasterEpoch());
        assertEquals(Integer.valueOf(1), replicaInfo().getMasterEpoch());
        assertEquals(1, result.getEvents().size());

        apply(result);

        assertEquals(Integer.valueOf(2), replicaInfo().getMasterEpoch());
        assertEquals(before.getSyncStateSet(), syncStateSet().getSyncStateSet());
        assertEquals(before.getSyncStateSetEpoch(), syncStateSet().getSyncStateSetEpoch());
        // Replaying the same accepted startup cannot grant a second epoch.
        apply(result);
        assertEquals(Integer.valueOf(2), replicaInfo().getMasterEpoch());
    }

    @Test
    public void testRegistrationRetryAndSnapshotRestoreKeepTheSameGrant() throws Throwable {
        apply(register(1L, MASTER_ADDRESS, 2L, "second-master", ALL_ALIVE));
        ReplicasInfoManager restored = newManager();
        restored.deserializeFrom(manager.serialize());
        manager = restored;

        ControllerResult<RegisterBrokerToControllerResponseHeader> retry =
            register(1L, MASTER_ADDRESS, 2L, "second-master", NONE_ALIVE);
        assertEquals(ResponseCode.SUCCESS, retry.getResponseCode());
        assertTrue(retry.getEvents().isEmpty());
        assertEquals(Integer.valueOf(2), retry.getResponse().getMasterEpoch());
        assertEquals("second-master", retry.getResponse().getBrokerStartupId());

        apply(register(1L, MASTER_ADDRESS, 3L, "third-master", ALL_ALIVE));
        assertEquals(Integer.valueOf(3), replicaInfo().getMasterEpoch());
    }

    @Test
    public void testStaleConflictingAndLegacyRegistrationsCannotReplaceStartup() throws Throwable {
        apply(register(1L, MASTER_ADDRESS, 2L, "second-master", ALL_ALIVE));
        ReplicasInfoManager restored = newManager();
        restored.deserializeFrom(manager.serialize());
        manager = restored;

        assertRejected(register(1L, "127.0.0.1:30911", 1L, "first-master", ALL_ALIVE));
        assertRejected(register(1L, "127.0.0.1:30911", 2L, "different-startup", ALL_ALIVE));
        assertRejected(register(1L, "127.0.0.1:30911", null, null, ALL_ALIVE));

        assertEquals(Integer.valueOf(2), replicaInfo().getMasterEpoch());
        assertEquals(MASTER_ADDRESS, replicaInfo().getMasterAddress());
        assertEquals(ResponseCode.SUCCESS, register(1L, MASTER_ADDRESS, 2L, "second-master", ALL_ALIVE).getResponseCode());
    }

    @Test
    public void testFollowerRestartDoesNotAdvanceTheMasterEpoch() {
        ControllerResult<RegisterBrokerToControllerResponseHeader> result =
            register(2L, SLAVE_ADDRESS, 2L, "second-slave", ALL_ALIVE);
        apply(result);

        assertEquals(Long.valueOf(1L), result.getResponse().getMasterBrokerId());
        assertEquals(Integer.valueOf(1), result.getResponse().getMasterEpoch());
        assertEquals(Integer.valueOf(1), replicaInfo().getMasterEpoch());
        assertEquals(new HashSet<>(Arrays.asList(1L, 2L)), syncStateSet().getSyncStateSet());
        assertTrue(register(2L, SLAVE_ADDRESS, 2L, "second-slave", ALL_ALIVE).getEvents().isEmpty());
    }

    @Test
    public void testRegistrationReportsEpochEvenWhenMasterIsInactive() {
        ControllerResult<RegisterBrokerToControllerResponseHeader> result =
            register(2L, SLAVE_ADDRESS, 2L, "second-slave", NONE_ALIVE);
        apply(result);

        assertNull(result.getResponse().getMasterBrokerId());
        assertEquals(Integer.valueOf(1), result.getResponse().getMasterEpoch());
        assertEquals(Long.valueOf(2L), result.getResponse().getBrokerStartupGeneration());
        assertEquals("second-slave", result.getResponse().getBrokerStartupId());
    }

    @Test
    public void testFirstStartupIdentityOnLegacyMasterCreatesNewEpoch() {
        manager = newManager();
        apply(manager.applyBrokerId(new ApplyBrokerIdRequestHeader(CLUSTER, BROKER, 1L, MASTER_ADDRESS + ";1")));
        ControllerResult<RegisterBrokerToControllerResponseHeader> legacy = register(1L, MASTER_ADDRESS, null, null, ALL_ALIVE);
        apply(legacy);
        assertNull(legacy.getResponse().getBrokerStartupGeneration());
        assertNull(legacy.getResponse().getBrokerStartupId());
        apply(manager.electMaster(ElectMasterRequestHeader.ofBrokerTrigger(CLUSTER, BROKER, 1L),
            new DefaultElectPolicy(ALL_ALIVE, null)));

        ControllerResult<RegisterBrokerToControllerResponseHeader> upgraded =
            register(1L, MASTER_ADDRESS, 1L, "first-upgraded-startup", ALL_ALIVE);
        apply(upgraded);

        assertEquals(Integer.valueOf(2), upgraded.getResponse().getMasterEpoch());
        assertEquals(Integer.valueOf(2), replicaInfo().getMasterEpoch());
    }

    @Test
    public void testMalformedStartupIdentityCannotMutateState() {
        assertRejected(register(1L, MASTER_ADDRESS, null, "missing-generation", ALL_ALIVE));
        assertRejected(register(1L, MASTER_ADDRESS, 2L, null, ALL_ALIVE));
        assertRejected(register(1L, MASTER_ADDRESS, 0L, "zero-generation", ALL_ALIVE));
        assertRejected(register(1L, MASTER_ADDRESS, 2L, " ", ALL_ALIVE));
        assertEquals(Integer.valueOf(1), replicaInfo().getMasterEpoch());
    }

    private ReplicasInfoManager newManager() {
        ControllerConfig config = new ControllerConfig();
        return raft ? new RaftReplicasInfoManager(config) : new ReplicasInfoManager(config);
    }

    private ControllerResult<RegisterBrokerToControllerResponseHeader> register(long brokerId, String address,
        Long generation, String startupId, BrokerValidPredicate alive) {
        RegisterBrokerToControllerRequestHeader request =
            new RegisterBrokerToControllerRequestHeader(CLUSTER, BROKER, brokerId, address);
        request.setBrokerStartupGeneration(generation);
        request.setBrokerStartupId(startupId);
        return manager.registerBroker(request, alive);
    }

    private void apply(ControllerResult<?> result) {
        assertEquals(result.getRemark(), ResponseCode.SUCCESS, result.getResponseCode());
        for (EventMessage event : ListEventSerializer.deserialize(ListEventSerializer.serialize(result.getEvents(), null), null)) {
            EventMessage decoded = serializer.deserialize(serializer.serialize(event));
            assertNotNull(decoded);
            manager.applyEvent(decoded);
        }
    }

    private void assertRejected(ControllerResult<?> result) {
        assertEquals(ResponseCode.CONTROLLER_INVALID_REQUEST, result.getResponseCode());
        assertFalse(result.getRemark().isEmpty());
        assertTrue(result.getEvents().isEmpty());
    }

    private GetReplicaInfoResponseHeader replicaInfo() {
        return manager.getReplicaInfo(new GetReplicaInfoRequestHeader(BROKER)).getResponse();
    }

    private SyncStateSet syncStateSet() {
        return RemotingSerializable.decode(manager.getReplicaInfo(new GetReplicaInfoRequestHeader(BROKER)).getBody(), SyncStateSet.class);
    }
}
