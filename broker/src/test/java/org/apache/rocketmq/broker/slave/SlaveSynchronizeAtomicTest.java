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
package org.apache.rocketmq.broker.slave;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.processor.QueryAssignmentProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SlaveSynchronizeAtomicTest {
    @Spy
    private BrokerController brokerController =
            new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(),
                    new MessageStoreConfig());

    private SlaveSynchronize slaveSynchronize;

    @Mock
    private BrokerOuterAPI brokerOuterAPI;

    @Mock
    private TopicConfigManager topicConfigManager;


    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Mock
    private QueryAssignmentProcessor queryAssignmentProcessor;

    @Mock
    private MessageRequestModeManager messageRequestModeManager;


    private static final String BROKER_ADDR = "127.0.0.1:10911";
    private final SubscriptionGroupWrapper subscriptionGroupWrapper = createSubscriptionGroupWrapper();
    private final MessageRequestModeSerializeWrapper requestModeSerializeWrapper = createMessageRequestModeWrapper();
    private final DataVersion dataVersion = new DataVersion();

    @Before
    public void init() {
        for (int i = 0; i < 100000; i++) {
            subscriptionGroupWrapper.getSubscriptionGroupTable().put("group" + i, new SubscriptionGroupConfig());
        }
        for (int i = 0; i < 100000; i++) {
            requestModeSerializeWrapper.getMessageRequestModeMap().put("topic" + i, new ConcurrentHashMap<>());
        }
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(subscriptionGroupManager.getDataVersion()).thenReturn(dataVersion);
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(
                subscriptionGroupWrapper.getSubscriptionGroupTable());
        slaveSynchronize = new SlaveSynchronize(brokerController);
        slaveSynchronize.setMasterAddr(BROKER_ADDR);
    }

    private SubscriptionGroupWrapper createSubscriptionGroupWrapper() {
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        wrapper.setSubscriptionGroupTable(new ConcurrentHashMap<>());
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        return wrapper;
    }

    private MessageRequestModeSerializeWrapper createMessageRequestModeWrapper() {
        MessageRequestModeSerializeWrapper wrapper = new MessageRequestModeSerializeWrapper();
        wrapper.setMessageRequestModeMap(new ConcurrentHashMap<>());
        return wrapper;
    }

    @Test
    public void testSyncAtomically()
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException,
            InterruptedException {
        when(brokerOuterAPI.getAllSubscriptionGroupConfig(anyString())).thenReturn(subscriptionGroupWrapper);
        when(brokerOuterAPI.getAllMessageRequestMode(anyString())).thenReturn(requestModeSerializeWrapper);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            while (countDownLatch.getCount() > 0) {
                dataVersion.nextVersion();
                try {
                    slaveSynchronize.syncAll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        for (int i = 0; i < 10000000; i++) {
            Assert.assertTrue(subscriptionGroupWrapper.getSubscriptionGroupTable()
                    .containsKey("group" + ThreadLocalRandom.current().nextInt(0, 100000)));
            Assert.assertTrue(requestModeSerializeWrapper.getMessageRequestModeMap()
                    .containsKey("topic" + ThreadLocalRandom.current().nextInt(0, 100000)));
        }
        countDownLatch.countDown();
    }
}
