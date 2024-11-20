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
package org.apache.rocketmq.broker.offset;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BroadcastOffsetManagerTest {

    private final AtomicLong maxOffset = new AtomicLong(10L);
    private final AtomicLong commitOffset = new AtomicLong(-1);

    private final ConsumerOffsetManager consumerOffsetManager = mock(ConsumerOffsetManager.class);
    private final ConsumerManager consumerManager = mock(ConsumerManager.class);
    private final BrokerConfig brokerConfig = new BrokerConfig();
    private final Set<String> onlineClientIdSet = new HashSet<>();
    private BroadcastOffsetManager broadcastOffsetManager;

    @Before
    public void before() throws ConsumeQueueException {
        brokerConfig.setEnableBroadcastOffsetStore(true);
        brokerConfig.setBroadcastOffsetExpireSecond(1);
        brokerConfig.setBroadcastOffsetExpireMaxSecond(5);
        BrokerController brokerController = mock(BrokerController.class);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);

        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        doAnswer((Answer<ClientChannelInfo>) mock -> {
            String clientId = mock.getArgument(1);
            if (onlineClientIdSet.contains(clientId)) {
                return new ClientChannelInfo(null);
            }
            return null;
        }).when(consumerManager).findChannel(anyString(), anyString());

        doAnswer((Answer<Long>) mock -> commitOffset.get())
            .when(consumerOffsetManager).queryOffset(anyString(), anyString(), anyInt());
        doAnswer((Answer<Void>) mock -> {
            commitOffset.set(mock.getArgument(4));
            return null;
        }).when(consumerOffsetManager).commitOffset(anyString(), anyString(), anyString(), anyInt(), anyLong());
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);

        MessageStore messageStore = mock(MessageStore.class);
        doAnswer((Answer<Long>) mock -> maxOffset.get())
            .when(messageStore).getMaxOffsetInQueue(anyString(), anyInt(), anyBoolean());
        when(brokerController.getMessageStore()).thenReturn(messageStore);

        broadcastOffsetManager = new BroadcastOffsetManager(brokerController);
    }

    @Test
    public void testBroadcastOffsetSwitch() throws ConsumeQueueException {
        // client1 connect to broker
        onlineClientIdSet.add("client1");
        long offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client1", 0, false);
        Assert.assertEquals(-1, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 10, "client1", false);
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client1", 11, false);
        Assert.assertEquals(-1, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 11, "client1", false);

        // client1 connect to proxy
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client1", -1, true);
        Assert.assertEquals(11, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 11, "client1", true);
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client1", 11, true);
        Assert.assertEquals(-1, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 12, "client1", true);

        broadcastOffsetManager.scanOffsetData();
        Assert.assertEquals(12L, commitOffset.get());

        // client2 connect to proxy
        onlineClientIdSet.add("client2");
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client2", -1, true);
        Assert.assertEquals(12, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 12, "client2", true);
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client2", 11, true);
        Assert.assertEquals(-1, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 13, "client2", true);

        broadcastOffsetManager.scanOffsetData();
        Assert.assertEquals(12L, commitOffset.get());

        // client1 connect to broker
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client1", 20, false);
        Assert.assertEquals(12, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 12, "client1", false);
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client1", 12, false);
        Assert.assertEquals(-1, offset);

        onlineClientIdSet.clear();

        maxOffset.set(30L);

        // client3 connect to broker
        onlineClientIdSet.add("client3");
        offset = broadcastOffsetManager.queryInitOffset("group", "topic", 0, "client3", 30, false);
        Assert.assertEquals(-1, offset);
        broadcastOffsetManager.updateOffset("group", "topic", 0, 30, "client3", false);

        await().atMost(Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireSecond() + 1)).until(() -> {
            broadcastOffsetManager.scanOffsetData();
            return commitOffset.get() == 30L;
        });
    }

    @Test
    public void testBroadcastOffsetExpire() {
        onlineClientIdSet.add("client1");
        broadcastOffsetManager.updateOffset(
            "group", "topic", 0, 10, "client1", false);
        onlineClientIdSet.clear();

        await().atMost(Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireSecond() + 1)).until(() -> {
            broadcastOffsetManager.scanOffsetData();
            return broadcastOffsetManager.offsetStoreMap.isEmpty();
        });

        onlineClientIdSet.add("client1");
        broadcastOffsetManager.updateOffset(
            "group", "topic", 0, 10, "client1", false);
        await().atMost(Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireMaxSecond() + 1)).until(() -> {
            broadcastOffsetManager.scanOffsetData();
            return broadcastOffsetManager.offsetStoreMap.isEmpty();
        });
    }
}
