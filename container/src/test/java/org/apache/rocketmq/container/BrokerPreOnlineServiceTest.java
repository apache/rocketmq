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

package org.apache.rocketmq.container;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPreOnlineService;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BrokerPreOnlineServiceTest {
    @Mock
    private BrokerContainer brokerContainer;

    private InnerBrokerController innerBrokerController;

    @Mock
    private BrokerOuterAPI brokerOuterAPI;

    public void init(final long brokerId) throws Exception {
        when(brokerContainer.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);

        BrokerMemberGroup brokerMemberGroup1 = new BrokerMemberGroup();
        Map<Long, String> brokerAddrMap = new HashMap<>();
        brokerAddrMap.put(0L, "127.0.0.1:10911");
        brokerMemberGroup1.setBrokerAddrs(brokerAddrMap);

        BrokerMemberGroup brokerMemberGroup2 = new BrokerMemberGroup();
        brokerMemberGroup2.setBrokerAddrs(new HashMap<>());
        when(brokerOuterAPI.syncBrokerMemberGroup(anyString(), anyString(), anyBoolean()))
                .thenReturn(brokerMemberGroup1)
                .thenReturn(brokerMemberGroup2);
        BrokerSyncInfo brokerSyncInfo = mock(BrokerSyncInfo.class);
        when(brokerOuterAPI.retrieveBrokerHaInfo(anyString())).thenReturn(brokerSyncInfo);

        DefaultMessageStore defaultMessageStore = mock(DefaultMessageStore.class);
        when(defaultMessageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(brokerId);
        when(defaultMessageStore.getBrokerConfig()).thenReturn(brokerConfig);

//        HAService haService = new DefaultHAService();
//        haService.init(defaultMessageStore);
//        haService.start();
//
//        when(defaultMessageStore.getHaService()).thenReturn(haService);

        innerBrokerController = new InnerBrokerController(brokerContainer,
            defaultMessageStore.getBrokerConfig(),
            defaultMessageStore.getMessageStoreConfig());

        innerBrokerController.setTransactionalMessageCheckService(new TransactionalMessageCheckService(innerBrokerController));

        Field field = BrokerController.class.getDeclaredField("isIsolated");
        field.setAccessible(true);
        field.set(innerBrokerController, true);

        field = BrokerController.class.getDeclaredField("messageStore");
        field.setAccessible(true);
        field.set(innerBrokerController, defaultMessageStore);
    }

    @Test
    public void testMasterOnlineConnTimeout() throws Exception {
        init(0);
        BrokerPreOnlineService brokerPreOnlineService = new BrokerPreOnlineService(innerBrokerController);

        brokerPreOnlineService.start();

        await().atMost(Duration.ofSeconds(30)).until(() -> !innerBrokerController.isIsolated());
    }

    @Test
    public void testMasterOnlineNormal() throws Exception {
        init(0);
        BrokerPreOnlineService brokerPreOnlineService = new BrokerPreOnlineService(innerBrokerController);
        brokerPreOnlineService.start();
        TimeUnit.SECONDS.sleep(1);
        brokerPreOnlineService.shutdown();
        await().atMost(Duration.ofSeconds(30)).until(brokerPreOnlineService::isStopped);
    }

    @Test
    public void testSlaveOnlineNormal() throws Exception {
        init(1);
        BrokerPreOnlineService brokerPreOnlineService = new BrokerPreOnlineService(innerBrokerController);
        brokerPreOnlineService.start();
        TimeUnit.SECONDS.sleep(1);
        brokerPreOnlineService.shutdown();
        await().atMost(Duration.ofSeconds(30)).until(brokerPreOnlineService::isStopped);
    }

    @Test
    public void testGetServiceName() throws Exception {
        init(1);
        BrokerPreOnlineService brokerPreOnlineService = new BrokerPreOnlineService(innerBrokerController);
        assertEquals(BrokerPreOnlineService.class.getSimpleName(), brokerPreOnlineService.getServiceName());
    }
}
