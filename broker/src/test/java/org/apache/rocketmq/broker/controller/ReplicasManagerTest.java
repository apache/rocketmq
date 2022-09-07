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

package org.apache.rocketmq.broker.controller;

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplicasManagerTest {

    @Mock
    private BrokerController brokerController;

    private ReplicasManager replicasManager;

    @Mock
    private DefaultMessageStore defaultMessageStore;

    private SlaveSynchronize slaveSynchronize;

    private AutoSwitchHAService autoSwitchHAService;

    private MessageStoreConfig messageStoreConfig;

    private GetMetaDataResponseHeader getMetaDataResponseHeader;

    private BrokerConfig brokerConfig;

    @Mock
    private BrokerOuterAPI brokerOuterAPI;

    private RegisterBrokerToControllerResponseHeader registerBrokerToControllerResponseHeader;

    private Pair<GetReplicaInfoResponseHeader, SyncStateSet> result;

    private GetReplicaInfoResponseHeader getReplicaInfoResponseHeader;

    private SyncStateSet syncStateSet;

    private static final String OLD_MASTER_ADDRESS = "192.168.1.1";

    private static final String NEW_MASTER_ADDRESS = "192.168.1.2";

    private static final long MASTER_BROKER_ID = 0;

    private static final long SLAVE_BROKER_ID = 2;

    private static final int OLD_MASTER_EPOCH = 2;
    private static final int NEW_MASTER_EPOCH = 3;

    private static final String GROUP = "DEFAULT_GROUP";

    private static final String LEADER_ID = "leader-1";

    private static final Boolean IS_LEADER = true;

    private static final String PEERS = "1.1.1.1";

    private static final long SCHEDULE_SERVICE_EXEC_PERIOD = 5;

    private static final String SYNC_STATE = "1";

    @Before
    public void before() throws Exception {
        autoSwitchHAService = new AutoSwitchHAService();
        messageStoreConfig = new MessageStoreConfig();
        brokerConfig = new BrokerConfig();
        slaveSynchronize = new SlaveSynchronize(brokerController);
        getMetaDataResponseHeader = new GetMetaDataResponseHeader(GROUP, LEADER_ID, OLD_MASTER_ADDRESS, IS_LEADER, PEERS);
        registerBrokerToControllerResponseHeader = new RegisterBrokerToControllerResponseHeader();
        registerBrokerToControllerResponseHeader.setMasterAddress(OLD_MASTER_ADDRESS);
        getReplicaInfoResponseHeader = new GetReplicaInfoResponseHeader();
        getReplicaInfoResponseHeader.setMasterAddress(OLD_MASTER_ADDRESS);
        getReplicaInfoResponseHeader.setBrokerId(MASTER_BROKER_ID);
        getReplicaInfoResponseHeader.setMasterEpoch(NEW_MASTER_EPOCH);
        syncStateSet = new SyncStateSet(Sets.newLinkedHashSet(SYNC_STATE), NEW_MASTER_EPOCH);
        result = new Pair<>(getReplicaInfoResponseHeader, syncStateSet);
        when(defaultMessageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        when(brokerController.getMessageStore().getHaService()).thenReturn(autoSwitchHAService);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getSlaveSynchronize()).thenReturn(slaveSynchronize);
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerController.getBrokerAddr()).thenReturn(OLD_MASTER_ADDRESS);
        when(brokerOuterAPI.getControllerMetaData(any())).thenReturn(getMetaDataResponseHeader);
        when(brokerOuterAPI.registerBrokerToController(any(), any(), any(), any(), anyInt(), anyLong())).thenReturn(registerBrokerToControllerResponseHeader);
        when(brokerOuterAPI.getReplicaInfo(any(), any(), any())).thenReturn(result);
        replicasManager = new ReplicasManager(brokerController);
        autoSwitchHAService.init(defaultMessageStore);
        replicasManager.start();
        // execute schedulingSyncBrokerMetadata()
        TimeUnit.SECONDS.sleep(SCHEDULE_SERVICE_EXEC_PERIOD);
    }

    @After
    public void after() {
        replicasManager.shutdown();
        brokerController.shutdown();
    }

    @Test
    public void changeBrokerRoleTest(){
        // not equal to localAddress
        Assertions.assertThatCode(() -> replicasManager.changeBrokerRole(NEW_MASTER_ADDRESS, NEW_MASTER_EPOCH, OLD_MASTER_EPOCH, SLAVE_BROKER_ID))
            .doesNotThrowAnyException();

        // equal to localAddress
        Assertions.assertThatCode(() -> replicasManager.changeBrokerRole(OLD_MASTER_ADDRESS, NEW_MASTER_EPOCH, OLD_MASTER_EPOCH , SLAVE_BROKER_ID))
            .doesNotThrowAnyException();
    }

    @Test
    public void changeToMasterTest() {
        Assertions.assertThatCode(() -> replicasManager.changeToMaster(NEW_MASTER_EPOCH, OLD_MASTER_EPOCH)).doesNotThrowAnyException();
    }

    @Test
    public void changeToSlaveTest() {
        Assertions.assertThatCode(() -> replicasManager.changeToSlave(NEW_MASTER_ADDRESS, NEW_MASTER_EPOCH, MASTER_BROKER_ID))
            .doesNotThrowAnyException();
    }
}
