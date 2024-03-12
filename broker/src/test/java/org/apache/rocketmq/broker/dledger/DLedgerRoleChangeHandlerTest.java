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
package org.apache.rocketmq.broker.dledger;

import io.openmessaging.storage.dledger.DLedgerServer;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BrokerController.class, SlaveSynchronize.class, BrokerOuterAPI.class})
public class DLedgerRoleChangeHandlerTest {
    @Spy
    private BrokerController brokerController = new BrokerController(
            new BrokerConfig(),
            new NettyServerConfig(),
            new NettyClientConfig(),
            new MessageStoreConfig()
    );

    @Mock
    DefaultMessageStore messageStore;

    @Mock
    DLedgerCommitLog commitLog;

    @Mock
    private NettyRemotingClient nettyRemotingClient;

    DLedgerRoleChangeHandler roleChangeHandler;

    @Before
    public void setUp() throws NoSuchFieldException, IOException, IllegalAccessException {
        when(messageStore.getCommitLog()).thenReturn(commitLog);
        when(commitLog.getdLedgerServer()).thenReturn(mock(DLedgerServer.class));
        roleChangeHandler = new DLedgerRoleChangeHandler(brokerController, messageStore);

        Field field = BrokerOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(brokerController.getBrokerOuterAPI(), nettyRemotingClient);

        brokerController.setMessageStore(new DefaultMessageStore(
                brokerController.getMessageStoreConfig(),
                new BrokerStatsManager(brokerController.getBrokerConfig().getBrokerClusterName(), false),
                (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> { },
                brokerController.getBrokerConfig(),
                new ConcurrentHashMap<>()));
    }

    @After
    public void tearDown() {
        roleChangeHandler.shutdown();
    }

    @Test
    public void dLedgerSynchronizeOffsetFromPreMaster() throws InterruptedException, NoSuchFieldException,
            IllegalAccessException, RemotingSendRequestException, RemotingConnectException, RemotingTimeoutException, MQBrokerException {
        // stub for registerBrokerAll
        List<String> namesrvList = new ArrayList<>();
        namesrvList.add("127.0.0.1:65535");
        when(nettyRemotingClient.getAvailableNameSrvList()).thenReturn(namesrvList);

        // stub for registerBroker
        RegisterBrokerResponseHeader responseHeader = new RegisterBrokerResponseHeader();
        responseHeader.setMasterAddr("127.0.0.1:1");
        RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("masterAddr", "127.0.0.1:1");
        response.setExtFields(extFields);
        response.setCode(ResponseCode.SUCCESS);
        when(nettyRemotingClient.invokeSync(eq("127.0.0.1:65535"), any(), eq(24000L))).thenReturn(response);

        // stub for syncAll
        PowerMockito.suppress(PowerMockito.method(SlaveSynchronize.class, "syncTopicConfig"));
        PowerMockito.suppress(PowerMockito.method(SlaveSynchronize.class, "syncDelayOffset"));
        PowerMockito.suppress(PowerMockito.method(SlaveSynchronize.class, "syncSubscriptionGroupConfig"));
        PowerMockito.suppress(PowerMockito.method(SlaveSynchronize.class, "syncMessageRequestMode"));

        // stub for syncConsumerOffset
        ConsumerOffsetSerializeWrapper offsetWrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<Integer, Long> queueTable = new ConcurrentHashMap<>();
        queueTable.put(0, 1000L);
        ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        offsetTable.put("topic1@group1", queueTable);
        offsetWrapper.setOffsetTable(offsetTable);
        offsetWrapper.setDataVersion(new DataVersion());
        BrokerOuterAPI brokerOuterAPI = mock(BrokerOuterAPI.class);
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerOuterAPI.getAllConsumerOffset("127.0.0.1:1")).thenReturn(offsetWrapper);

        assertEquals(0, brokerController.getConsumerOffsetManager().getOffsetTable().size());
        SlaveSynchronize slaveSynchronize = brokerController.getSlaveSynchronize();
        Field field = SlaveSynchronize.class.getDeclaredField("brokerController");
        field.setAccessible(true);
        field.set(slaveSynchronize, brokerController);

        Field field1 = BrokerController.class.getDeclaredField("scheduledExecutorService");
        field1.setAccessible(true);
        field1.set(brokerController, Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
                "BrokerControllerScheduledThread")));

        // 1. reset masterAddr
        // 2. start to call syncAll periodically after 3 seconds
        // 3. register and get masterAddr
        // 4. sync once immediately
        assertNull(brokerController.getSlaveSynchronize().getMasterAddr());
        roleChangeHandler.changeToSlave(1);
        assertEquals("127.0.0.1:1", brokerController.getSlaveSynchronize().getMasterAddr());

        TimeUnit.SECONDS.sleep(5);
        assertEquals(Long.valueOf(1000L), brokerController.getConsumerOffsetManager().getOffsetTable().get("topic1@group1").get(0));

    }

}
