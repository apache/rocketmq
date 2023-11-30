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

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;


import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.RunningFlags;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplicasManagerTest {

    public static final String STORE_BASE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "ReplicasManagerTest";

    public static final String STORE_PATH = STORE_BASE_PATH + File.separator + UUID.randomUUID();

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

    private GetNextBrokerIdResponseHeader getNextBrokerIdResponseHeader;

    private ApplyBrokerIdResponseHeader applyBrokerIdResponseHeader;

    private RegisterBrokerToControllerResponseHeader registerBrokerToControllerResponseHeader;

    private ElectMasterResponseHeader brokerTryElectResponseHeader;

    private Pair<GetReplicaInfoResponseHeader, SyncStateSet> result;

    private GetReplicaInfoResponseHeader getReplicaInfoResponseHeader;

    private SyncStateSet syncStateSet;

    private RunningFlags runningFlags = new RunningFlags();

    private static final String OLD_MASTER_ADDRESS = "192.168.1.1";

    private static final String NEW_MASTER_ADDRESS = "192.168.1.2";

    private static final long BROKER_ID_1 = 1;

    private static final long BROKER_ID_2 = 2;

    private static final int OLD_MASTER_EPOCH = 2;
    private static final int NEW_MASTER_EPOCH = 3;

    private static final String GROUP = "DEFAULT_GROUP";

    private static final String LEADER_ID = "leader-1";

    private static final Boolean IS_LEADER = true;

    private static final String PEERS = "1.1.1.1";

    private static final long SCHEDULE_SERVICE_EXEC_PERIOD = 5;

    private static final Long SYNC_STATE = 1L;

    private static final HashSet<Long> SYNC_STATE_SET_1 = new HashSet<Long>(Arrays.asList(BROKER_ID_1));

    private static final HashSet<Long> SYNC_STATE_SET_2 = new HashSet<Long>(Arrays.asList(BROKER_ID_2));

    @Before
    public void before() throws Exception {
        UtilAll.deleteFile(new File(STORE_BASE_PATH));
        autoSwitchHAService = new AutoSwitchHAService();
        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(STORE_PATH);
        brokerConfig = new BrokerConfig();
        slaveSynchronize = new SlaveSynchronize(brokerController);
        getMetaDataResponseHeader = new GetMetaDataResponseHeader(GROUP, LEADER_ID, OLD_MASTER_ADDRESS, IS_LEADER, PEERS);
        getNextBrokerIdResponseHeader = new GetNextBrokerIdResponseHeader();
        getNextBrokerIdResponseHeader.setNextBrokerId(BROKER_ID_1);
        applyBrokerIdResponseHeader = new ApplyBrokerIdResponseHeader();
        registerBrokerToControllerResponseHeader = new RegisterBrokerToControllerResponseHeader();
        brokerTryElectResponseHeader = new ElectMasterResponseHeader();
        brokerTryElectResponseHeader.setMasterBrokerId(BROKER_ID_1);
        brokerTryElectResponseHeader.setMasterAddress(OLD_MASTER_ADDRESS);
        brokerTryElectResponseHeader.setMasterEpoch(OLD_MASTER_EPOCH);
        brokerTryElectResponseHeader.setSyncStateSetEpoch(OLD_MASTER_EPOCH);
        getReplicaInfoResponseHeader = new GetReplicaInfoResponseHeader();
        getReplicaInfoResponseHeader.setMasterAddress(OLD_MASTER_ADDRESS);
        getReplicaInfoResponseHeader.setMasterBrokerId(BROKER_ID_1);
        getReplicaInfoResponseHeader.setMasterEpoch(NEW_MASTER_EPOCH);
        syncStateSet = new SyncStateSet(Sets.newLinkedHashSet(SYNC_STATE), NEW_MASTER_EPOCH);
        result = new Pair<>(getReplicaInfoResponseHeader, syncStateSet);
        TopicConfigManager topicConfigManager = new TopicConfigManager();
        when(defaultMessageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        when(brokerController.getMessageStore().getHaService()).thenReturn(autoSwitchHAService);
        when(brokerController.getMessageStore().getRunningFlags()).thenReturn(runningFlags);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getSlaveSynchronize()).thenReturn(slaveSynchronize);
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerController.getBrokerAddr()).thenReturn(OLD_MASTER_ADDRESS);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerOuterAPI.getControllerMetaData(any())).thenReturn(getMetaDataResponseHeader);
        when(brokerOuterAPI.checkAddressReachable(any())).thenReturn(true);
        when(brokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(getNextBrokerIdResponseHeader);
        when(brokerOuterAPI.applyBrokerId(any(), any(), anyLong(), any(), any())).thenReturn(applyBrokerIdResponseHeader);
        when(brokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenReturn(new Pair<>(new RegisterBrokerToControllerResponseHeader(), SYNC_STATE_SET_1));
        when(brokerOuterAPI.getReplicaInfo(any(), any())).thenReturn(result);
        when(brokerOuterAPI.brokerElect(any(), any(), any(), any())).thenReturn(new Pair<>(brokerTryElectResponseHeader, SYNC_STATE_SET_1));
        replicasManager = new ReplicasManager(brokerController);
        autoSwitchHAService.init(defaultMessageStore);
        replicasManager.start();
        // execute schedulingSyncBrokerMetadata()
        Awaitility.await().pollDelay(Duration.ofSeconds(SCHEDULE_SERVICE_EXEC_PERIOD)).until(() -> true);
    }

    @After
    public void after() {
        replicasManager.shutdown();
        brokerController.shutdown();
        UtilAll.deleteFile(new File(STORE_BASE_PATH));
    }

    @Test
    public void changeBrokerRoleTest() {
        HashSet<Long> syncStateSetA = new HashSet<>();
        syncStateSetA.add(BROKER_ID_1);
        HashSet<Long> syncStateSetB = new HashSet<>();
        syncStateSetA.add(BROKER_ID_2);
        // not equal to localAddress
        Assertions.assertThatCode(() -> replicasManager.changeBrokerRole(BROKER_ID_2, NEW_MASTER_ADDRESS, NEW_MASTER_EPOCH, OLD_MASTER_EPOCH, syncStateSetB))
                .doesNotThrowAnyException();

        // equal to localAddress
        Assertions.assertThatCode(() -> replicasManager.changeBrokerRole(BROKER_ID_1, OLD_MASTER_ADDRESS, NEW_MASTER_EPOCH, OLD_MASTER_EPOCH, syncStateSetA))
                .doesNotThrowAnyException();
    }

    @Test
    public void changeToMasterTest() {
        HashSet<Long> syncStateSet = new HashSet<>();
        syncStateSet.add(BROKER_ID_1);
        Assertions.assertThatCode(() -> replicasManager.changeToMaster(NEW_MASTER_EPOCH, OLD_MASTER_EPOCH, syncStateSet)).doesNotThrowAnyException();
    }

    @Test
    public void changeToSlaveTest() {
        Assertions.assertThatCode(() -> replicasManager.changeToSlave(NEW_MASTER_ADDRESS, NEW_MASTER_EPOCH, BROKER_ID_2))
                .doesNotThrowAnyException();
    }
}
