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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.broker.BrokerPreOnlineService;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BrokerPreOnlineTest {
    @Mock
    private BrokerContainer brokerContainer;

    private InnerBrokerController innerBrokerController;

    @Mock
    private BrokerOuterAPI brokerOuterAPI;

    public void init() throws Exception {
        when(brokerContainer.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);

        BrokerMemberGroup brokerMemberGroup1 = new BrokerMemberGroup();
        Map<Long, String> brokerAddrMap = new HashMap<>();
        brokerAddrMap.put(1L, "127.0.0.1:20911");
        brokerMemberGroup1.setBrokerAddrs(brokerAddrMap);

        BrokerMemberGroup brokerMemberGroup2 = new BrokerMemberGroup();
        brokerMemberGroup2.setBrokerAddrs(new HashMap<>());

        DefaultMessageStore defaultMessageStore = mock(DefaultMessageStore.class);
        when(defaultMessageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(defaultMessageStore.getBrokerConfig()).thenReturn(new BrokerConfig());

        innerBrokerController = new InnerBrokerController(brokerContainer,
            defaultMessageStore.getBrokerConfig(),
            defaultMessageStore.getMessageStoreConfig());

        innerBrokerController.getBrokerMessageService().setTransactionalMessageCheckService(new TransactionalMessageCheckService(innerBrokerController));
        innerBrokerController.setIsolated(true);
        innerBrokerController.setMessageStore(defaultMessageStore);
    }

    @Test
    public void testMasterOnlineConnTimeout() throws Exception {
        init();
        BrokerPreOnlineService brokerPreOnlineService = new BrokerPreOnlineService(innerBrokerController);

        brokerPreOnlineService.start();

        await().atMost(Duration.ofSeconds(30)).until(() -> !innerBrokerController.isIsolated());
    }
}
