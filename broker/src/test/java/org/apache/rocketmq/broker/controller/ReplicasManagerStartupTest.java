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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.RunningFlags;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicasManagerStartupTest {
    private ReplicasManager manager;
    private BrokerController controller;
    private AutoSwitchHAService ha;
    private RunningFlags flags;
    private MessageStoreConfig storeConfig;

    @Before
    public void setUp() throws Exception {
        controller = mock(BrokerController.class);
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        ha = mock(AutoSwitchHAService.class);
        flags = new RunningFlags();
        storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerRole(BrokerRole.SLAVE);
        BrokerConfig config = new BrokerConfig();
        config.setBrokerId(1);
        when(controller.getBrokerConfig()).thenReturn(config);
        when(controller.getMessageStore()).thenReturn(store);
        when(controller.getMessageStoreConfig()).thenReturn(storeConfig);
        when(controller.getBrokerAddr()).thenReturn("127.0.0.1:10911");
        when(controller.getSlaveSynchronize()).thenReturn(mock(SlaveSynchronize.class));
        when(controller.getTopicConfigManager()).thenReturn(new TopicConfigManager());
        when(store.getHaService()).thenReturn(ha);
        when(store.getRunningFlags()).thenReturn(flags);
        when(ha.maybeShrinkSyncStateSet()).thenReturn(new HashSet<>(Collections.singleton(1L)));
        manager = new ReplicasManager(controller);
        setField("brokerControllerId", 1L);
    }

    @After
    public void tearDown() {
        manager.shutdown();
    }

    @Test
    public void refusesMasterNotificationBeforeStartupRegistration() throws Exception {
        manager.changeBrokerRole(1L, "127.0.0.1:10911", 2, 1, Collections.singleton(1L));
        verify(ha, never()).changeToMaster(2);
        assertEquals(0, manager.getMasterEpoch());
    }

    @Test
    public void failedHaTransitionStaysFencedAndSameGrantCanRetry() throws Exception {
        setField("startupRegistered", true);
        setField("startupMasterEpoch", 2);
        when(ha.changeToMaster(2)).thenReturn(false);
        try {
            manager.changeToMaster(2, 1, Collections.singleton(1L));
            fail("Failed HA transition must not publish the master role");
        } catch (IllegalStateException expected) {
            assertTrue(flags.isFenced());
        }
        assertEquals(0, manager.getMasterEpoch());
        assertEquals(BrokerRole.SLAVE, storeConfig.getBrokerRole());
        verify(controller, never()).changeSpecialServiceStatus(true);

        when(ha.changeToMaster(2)).thenReturn(true);
        manager.changeToMaster(2, 1, Collections.singleton(1L));
        assertEquals(2, manager.getMasterEpoch());
        assertEquals(BrokerRole.SYNC_MASTER, storeConfig.getBrokerRole());
        assertFalse(flags.isFenced());
        verify(controller).changeSpecialServiceStatus(true);
    }

    @Test
    public void refusesRoleOlderThanRegistrationSnapshot() throws Exception {
        setField("startupRegistered", true);
        setField("startupMasterEpoch", 3);
        manager.changeBrokerRole(1L, "127.0.0.1:10911", 2, 1, Collections.singleton(1L));
        verify(ha, never()).changeToMaster(2);
        assertEquals(0, manager.getMasterEpoch());
    }

    @Test(expected = IllegalStateException.class)
    public void recoveredStoreCannotReuseItsLastEpoch() throws Exception {
        setField("startupRegistered", true);
        setField("startupMasterEpoch", 3);
        setField("recoveredEpoch", 3);
        manager.changeToMaster(3, 1, Collections.singleton(1L));
    }

    private void setField(String name, Object value) throws Exception {
        Field field = ReplicasManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(manager, value);
    }
}
