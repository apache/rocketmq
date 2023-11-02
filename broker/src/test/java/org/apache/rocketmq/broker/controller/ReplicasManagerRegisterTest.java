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

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.RunningFlags;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.autoswitch.BrokerMetadata;
import org.apache.rocketmq.store.ha.autoswitch.TempBrokerMetadata;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ReplicasManager.class)
public class ReplicasManagerRegisterTest {

    public static final String STORE_BASE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "ReplicasManagerRegisterTest";

    public static final String STORE_PATH = STORE_BASE_PATH + File.separator + UUID.randomUUID();

    public static final String BROKER_NAME = "default-broker";

    public static final String CLUSTER_NAME = "default-cluster";

    public static final String NAME_SRV_ADDR = "127.0.0.1:9999";

    public static final String CONTROLLER_ADDR = "127.0.0.1:8888";

    public static final BrokerConfig BROKER_CONFIG;
    private final HashSet<Long> syncStateSet = new HashSet<>(Arrays.asList(1L));

    static {
        BROKER_CONFIG = new BrokerConfig();
        BROKER_CONFIG.setListenPort(21030);
        BROKER_CONFIG.setNamesrvAddr(NAME_SRV_ADDR);
        BROKER_CONFIG.setControllerAddr(CONTROLLER_ADDR);
        BROKER_CONFIG.setSyncControllerMetadataPeriod(2 * 1000);
        BROKER_CONFIG.setEnableControllerMode(true);
        BROKER_CONFIG.setBrokerName(BROKER_NAME);
        BROKER_CONFIG.setBrokerClusterName(CLUSTER_NAME);
    }

    private MessageStoreConfig buildMessageStoreConfig(int id) {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(STORE_PATH + File.separator + id);
        config.setStorePathCommitLog(config.getStorePathRootDir() + File.separator + "commitLog");
        config.setStorePathEpochFile(config.getStorePathRootDir() + File.separator + "epochFileCache");
        config.setStorePathBrokerIdentity(config.getStorePathRootDir() + File.separator + "brokerIdentity");
        return config;
    }

    private BrokerController mockedBrokerController;

    private DefaultMessageStore mockedMessageStore;

    private BrokerOuterAPI mockedBrokerOuterAPI;

    private AutoSwitchHAService mockedAutoSwitchHAService;

    private RunningFlags runningFlags = new RunningFlags();

    @Before
    public void setUp() throws Exception {
        UtilAll.deleteFile(new File(STORE_BASE_PATH));
        this.mockedBrokerController = Mockito.mock(BrokerController.class);
        this.mockedMessageStore = Mockito.mock(DefaultMessageStore.class);
        this.mockedBrokerOuterAPI = Mockito.mock(BrokerOuterAPI.class);
        this.mockedAutoSwitchHAService = Mockito.mock(AutoSwitchHAService.class);
        TopicConfigManager mockedTopicConfigManager = new TopicConfigManager();
        when(mockedBrokerController.getBrokerOuterAPI()).thenReturn(mockedBrokerOuterAPI);
        when(mockedBrokerController.getMessageStore()).thenReturn(mockedMessageStore);
        when(mockedBrokerController.getBrokerConfig()).thenReturn(BROKER_CONFIG);
        when(mockedBrokerController.getTopicConfigManager()).thenReturn(mockedTopicConfigManager);
        when(mockedMessageStore.getHaService()).thenReturn(mockedAutoSwitchHAService);
        when(mockedMessageStore.getRunningFlags()).thenReturn(runningFlags);
        when(mockedBrokerController.getSlaveSynchronize()).thenReturn(new SlaveSynchronize(mockedBrokerController));
        when(mockedBrokerOuterAPI.getControllerMetaData(any())).thenReturn(
                new GetMetaDataResponseHeader("default-group", "dledger-a", CONTROLLER_ADDR, true, CONTROLLER_ADDR));
        when(mockedBrokerOuterAPI.checkAddressReachable(any())).thenReturn(true);
        when(mockedBrokerController.getMessageStoreConfig()).thenReturn(buildMessageStoreConfig(0));
    }

    @Test
    public void testBrokerRegisterSuccess() throws Exception {

        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 1L));
        when(mockedBrokerOuterAPI.applyBrokerId(any(), any(), anyLong(), any(), any())).thenReturn(new ApplyBrokerIdResponseHeader());
        when(mockedBrokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenReturn(new Pair<>(new RegisterBrokerToControllerResponseHeader(),  syncStateSet));
        when(mockedBrokerOuterAPI.brokerElect(any(), any(), any(), anyLong())).thenReturn(new Pair<>(new ElectMasterResponseHeader(1L, "127.0.0.1:13131", 1, 1), syncStateSet));

        ReplicasManager replicasManager0 = new ReplicasManager(mockedBrokerController);
        replicasManager0.start();
        await().atMost(Duration.ofMillis(1000)).until(() ->
            replicasManager0.getState() == ReplicasManager.State.RUNNING
        );
        Assert.assertEquals(ReplicasManager.RegisterState.REGISTERED, replicasManager0.getRegisterState());
        Assert.assertEquals(1L, replicasManager0.getBrokerControllerId().longValue());
        checkMetadataFile(replicasManager0.getBrokerMetadata(), 1L);
        Assert.assertFalse(replicasManager0.getTempBrokerMetadata().isLoaded());
        Assert.assertFalse(replicasManager0.getTempBrokerMetadata().fileExists());
        replicasManager0.shutdown();
    }

    @Test
    public void testBrokerRegisterSuccessAndRestartWithChangedBrokerConfig() throws Exception {
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 1L));
        when(mockedBrokerOuterAPI.applyBrokerId(any(), any(), anyLong(), any(), any())).thenReturn(new ApplyBrokerIdResponseHeader());
        when(mockedBrokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenReturn(new Pair<>(new RegisterBrokerToControllerResponseHeader(),  syncStateSet));
        when(mockedBrokerOuterAPI.brokerElect(any(), any(), any(), anyLong())).thenReturn(new Pair<>(new ElectMasterResponseHeader(1L, "127.0.0.1:13131", 1, 1), syncStateSet));

        ReplicasManager replicasManager0 = new ReplicasManager(mockedBrokerController);
        replicasManager0.start();
        await().atMost(Duration.ofMillis(1000)).until(() ->
                replicasManager0.getState() == ReplicasManager.State.RUNNING
        );
        Assert.assertEquals(ReplicasManager.RegisterState.REGISTERED, replicasManager0.getRegisterState());
        Assert.assertEquals(1L, replicasManager0.getBrokerControllerId().longValue());
        checkMetadataFile(replicasManager0.getBrokerMetadata(), 1L);
        Assert.assertFalse(replicasManager0.getTempBrokerMetadata().isLoaded());
        Assert.assertFalse(replicasManager0.getTempBrokerMetadata().fileExists());
        replicasManager0.shutdown();

        // change broker name in broker config
        mockedBrokerController.getBrokerConfig().setBrokerName(BROKER_NAME + "1");
        ReplicasManager replicasManagerRestart = new ReplicasManager(mockedBrokerController);
        replicasManagerRestart.start();
        Assert.assertEquals(ReplicasManager.RegisterState.CREATE_METADATA_FILE_DONE, replicasManagerRestart.getRegisterState());
        mockedBrokerController.getBrokerConfig().setBrokerName(BROKER_NAME);
        replicasManagerRestart.shutdown();

        // change cluster name in broker config
        mockedBrokerController.getBrokerConfig().setBrokerClusterName(CLUSTER_NAME + "1");
        replicasManagerRestart = new ReplicasManager(mockedBrokerController);
        replicasManagerRestart.start();
        Assert.assertEquals(ReplicasManager.RegisterState.CREATE_METADATA_FILE_DONE, replicasManagerRestart.getRegisterState());
        mockedBrokerController.getBrokerConfig().setBrokerClusterName(CLUSTER_NAME);
        replicasManagerRestart.shutdown();
    }

    @Test
    public void testRegisterFailedAtGetNextBrokerId() throws Exception {
        ReplicasManager replicasManager = new ReplicasManager(mockedBrokerController);
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenThrow(new RuntimeException());

        replicasManager.start();

        Assert.assertEquals(ReplicasManager.State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE, replicasManager.getState());
        Assert.assertEquals(ReplicasManager.RegisterState.INITIAL, replicasManager.getRegisterState());
        Assert.assertFalse(replicasManager.getTempBrokerMetadata().fileExists());
        Assert.assertFalse(replicasManager.getBrokerMetadata().fileExists());
        Assert.assertNull(replicasManager.getBrokerControllerId());
        replicasManager.shutdown();
    }

    @Test
    public void testRegisterFailedAtCreateTempFile() throws Exception {
        ReplicasManager replicasManager = new ReplicasManager(mockedBrokerController);
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 1L));
        when(mockedBrokerOuterAPI.applyBrokerId(any(), any(), anyLong(), any(), any())).thenReturn(new ApplyBrokerIdResponseHeader());
        when(mockedBrokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenReturn(new Pair<>(new RegisterBrokerToControllerResponseHeader(),  syncStateSet));
        when(mockedBrokerOuterAPI.brokerElect(any(), any(), any(), anyLong())).thenReturn(new Pair<>(new ElectMasterResponseHeader(1L, "127.0.0.1:13131", 1, 1), syncStateSet));
        ReplicasManager spyReplicasManager = PowerMockito.spy(replicasManager);
        PowerMockito.doReturn(false).when(spyReplicasManager, "createTempMetadataFile", anyLong());

        spyReplicasManager.start();

        Assert.assertEquals(ReplicasManager.State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE, spyReplicasManager.getState());
        Assert.assertEquals(ReplicasManager.RegisterState.INITIAL, spyReplicasManager.getRegisterState());
        Assert.assertFalse(spyReplicasManager.getTempBrokerMetadata().fileExists());
        Assert.assertFalse(spyReplicasManager.getBrokerMetadata().fileExists());
        Assert.assertNull(spyReplicasManager.getBrokerControllerId());
        spyReplicasManager.shutdown();
    }

    @Test
    public void testRegisterFailedAtApplyBrokerIdFailed() throws Exception {
        ReplicasManager replicasManager = new ReplicasManager(mockedBrokerController);
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 1L));
        when(mockedBrokerOuterAPI.applyBrokerId(any(), any(), anyLong(), any(), any())).thenThrow(new RuntimeException());
        when(mockedBrokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenReturn(new Pair<>(new RegisterBrokerToControllerResponseHeader(),  syncStateSet));
        when(mockedBrokerOuterAPI.brokerElect(any(), any(), any(), anyLong())).thenReturn(new Pair<>(new ElectMasterResponseHeader(1L, "127.0.0.1:13131", 1, 1), syncStateSet));

        replicasManager.start();

        Assert.assertEquals(ReplicasManager.State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE, replicasManager.getState());
        Assert.assertNotEquals(ReplicasManager.RegisterState.CREATE_METADATA_FILE_DONE, replicasManager.getRegisterState());
        Assert.assertNotEquals(ReplicasManager.RegisterState.REGISTERED, replicasManager.getRegisterState());

        replicasManager.shutdown();

        Assert.assertFalse(replicasManager.getBrokerMetadata().fileExists());
        Assert.assertNull(replicasManager.getBrokerControllerId());
    }

    @Test
    public void testRegisterFailedAtCreateMetadataFileAndDeleteTemp() throws Exception {
        ReplicasManager replicasManager = new ReplicasManager(mockedBrokerController);
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 1L));
        when(mockedBrokerOuterAPI.applyBrokerId(any(), any(), anyLong(), any(), any())).thenReturn(new ApplyBrokerIdResponseHeader());
        when(mockedBrokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenReturn(new Pair<>(new RegisterBrokerToControllerResponseHeader(),  syncStateSet));
        when(mockedBrokerOuterAPI.brokerElect(any(), any(), any(), anyLong())).thenReturn(new Pair<>(new ElectMasterResponseHeader(1L, "127.0.0.1:13131", 1, 1), syncStateSet));

        ReplicasManager spyReplicasManager = PowerMockito.spy(replicasManager);
        PowerMockito.doReturn(false).when(spyReplicasManager, "createMetadataFileAndDeleteTemp");

        spyReplicasManager.start();

        Assert.assertEquals(ReplicasManager.State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE, spyReplicasManager.getState());
        Assert.assertEquals(ReplicasManager.RegisterState.CREATE_TEMP_METADATA_FILE_DONE, spyReplicasManager.getRegisterState());
        TempBrokerMetadata tempBrokerMetadata = spyReplicasManager.getTempBrokerMetadata();
        Assert.assertTrue(tempBrokerMetadata.fileExists());
        Assert.assertTrue(tempBrokerMetadata.isLoaded());
        Assert.assertFalse(spyReplicasManager.getBrokerMetadata().fileExists());
        Assert.assertNull(spyReplicasManager.getBrokerControllerId());

        spyReplicasManager.shutdown();

        // restart, we expect that this replicasManager still keep the tempMetadata and still try to finish its registering
        ReplicasManager replicasManagerNew = new ReplicasManager(mockedBrokerController);
        // because apply brokerId: 1 has succeeded, so now next broker id is 2
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 2L));

        replicasManagerNew.start();

        Assert.assertEquals(ReplicasManager.State.RUNNING, replicasManagerNew.getState());
        Assert.assertEquals(ReplicasManager.RegisterState.REGISTERED, replicasManagerNew.getRegisterState());
        // tempMetadata has been cleared
        Assert.assertFalse(replicasManagerNew.getTempBrokerMetadata().fileExists());
        Assert.assertFalse(replicasManagerNew.getTempBrokerMetadata().isLoaded());
        // metadata has been persisted
        Assert.assertTrue(replicasManagerNew.getBrokerMetadata().fileExists());
        Assert.assertTrue(replicasManagerNew.getBrokerMetadata().isLoaded());
        Assert.assertEquals(1L, replicasManagerNew.getBrokerMetadata().getBrokerId().longValue());
        Assert.assertEquals(1L, replicasManagerNew.getBrokerControllerId().longValue());
        replicasManagerNew.shutdown();
    }

    @Test
    public void testRegisterFailedAtRegisterSuccess() throws Exception {
        ReplicasManager replicasManager = new ReplicasManager(mockedBrokerController);
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 1L));
        when(mockedBrokerOuterAPI.applyBrokerId(any(), any(), anyLong(), any(), any())).thenReturn(new ApplyBrokerIdResponseHeader());
        when(mockedBrokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenThrow(new RuntimeException());
        when(mockedBrokerOuterAPI.brokerElect(any(), any(), any(), anyLong())).thenReturn(new Pair<>(new ElectMasterResponseHeader(1L, "127.0.0.1:13131", 1, 1), syncStateSet));

        replicasManager.start();

        Assert.assertEquals(ReplicasManager.State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE, replicasManager.getState());
        Assert.assertEquals(ReplicasManager.RegisterState.CREATE_METADATA_FILE_DONE, replicasManager.getRegisterState());
        TempBrokerMetadata tempBrokerMetadata = replicasManager.getTempBrokerMetadata();
        // temp metadata has been cleared
        Assert.assertFalse(tempBrokerMetadata.fileExists());
        Assert.assertFalse(tempBrokerMetadata.isLoaded());
        // metadata has been persisted
        Assert.assertTrue(replicasManager.getBrokerMetadata().fileExists());
        Assert.assertTrue(replicasManager.getBrokerMetadata().isLoaded());
        Assert.assertEquals(1L, replicasManager.getBrokerMetadata().getBrokerId().longValue());
        Assert.assertEquals(1L, replicasManager.getBrokerControllerId().longValue());

        replicasManager.shutdown();

        Mockito.reset(mockedBrokerOuterAPI);
        when(mockedBrokerOuterAPI.brokerElect(any(), any(), any(), anyLong())).thenReturn(new Pair<>(new ElectMasterResponseHeader(1L, "127.0.0.1:13131", 1, 1), syncStateSet));
        when(mockedBrokerOuterAPI.getControllerMetaData(any())).thenReturn(
                new GetMetaDataResponseHeader("default-group", "dledger-a", CONTROLLER_ADDR, true, CONTROLLER_ADDR));
        when(mockedBrokerOuterAPI.checkAddressReachable(any())).thenReturn(true);

        // restart, we expect that this replicasManager still keep the metadata and still try to finish its registering
        ReplicasManager replicasManagerNew = new ReplicasManager(mockedBrokerController);
        // because apply brokerId: 1 has succeeded, so now next broker id is 2
        when(mockedBrokerOuterAPI.getNextBrokerId(any(), any(), any())).thenReturn(new GetNextBrokerIdResponseHeader(CLUSTER_NAME, BROKER_NAME, 2L));
        // because apply brokerId: 1 has succeeded, so next request which try to apply brokerId: 1 will be failed
        when(mockedBrokerOuterAPI.applyBrokerId(any(), any(), eq(1L), any(), any())).thenThrow(new RuntimeException());
        when(mockedBrokerOuterAPI.registerBrokerToController(any(), any(), anyLong(), any(), any())).thenReturn(new Pair<>(new RegisterBrokerToControllerResponseHeader(),  syncStateSet));
        replicasManagerNew.start();

        Assert.assertEquals(ReplicasManager.State.RUNNING, replicasManagerNew.getState());
        Assert.assertEquals(ReplicasManager.RegisterState.REGISTERED, replicasManagerNew.getRegisterState());
        // tempMetadata has been cleared
        Assert.assertFalse(replicasManagerNew.getTempBrokerMetadata().fileExists());
        Assert.assertFalse(replicasManagerNew.getTempBrokerMetadata().isLoaded());
        // metadata has been persisted
        Assert.assertTrue(replicasManagerNew.getBrokerMetadata().fileExists());
        Assert.assertTrue(replicasManagerNew.getBrokerMetadata().isLoaded());
        Assert.assertEquals(1L, replicasManagerNew.getBrokerMetadata().getBrokerId().longValue());
        Assert.assertEquals(1L, replicasManagerNew.getBrokerControllerId().longValue());
        replicasManagerNew.shutdown();
    }


    private void checkMetadataFile(BrokerMetadata brokerMetadata0 ,Long brokerId) throws Exception {
        Assert.assertEquals(brokerId, brokerMetadata0.getBrokerId());
        Assert.assertTrue(brokerMetadata0.fileExists());
        BrokerMetadata brokerMetadata = new BrokerMetadata(brokerMetadata0.getFilePath());
        brokerMetadata.readFromFile();
        Assert.assertEquals(brokerMetadata0, brokerMetadata);
    }

    @After
    public void clear() {
        UtilAll.deleteFile(new File(STORE_BASE_PATH));
    }


}
