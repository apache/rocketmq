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
package org.apache.rocketmq.broker.pop;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PopConsumerServiceLockRetryTest {

    private static final String GROUP_ID = "groupId";
    private static final String TOPIC_ID = "topicId";
    private static final String ATTEMPT_ID = "attempt-id-lock-retry";
    private static final long INVISIBLE_TIME = 300_000L;

    private final String filePath = PopConsumerRocksdbStoreTest.getRandomStorePath();

    private BrokerController brokerController;
    private PopConsumerLockService consumerLockService;
    private SubscriptionGroupManager subscriptionGroupManager;
    private ConsumerOffsetManager consumerOffsetManager;
    private MessageStore messageStore;
    private PopConsumerService consumerService;

    @Before
    public void init() throws IOException, IllegalAccessException {
        BrokerConfig brokerConfig = new BrokerConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(filePath);

        TopicConfigManager topicConfigManager = Mockito.mock(TopicConfigManager.class);
        subscriptionGroupManager = Mockito.mock(SubscriptionGroupManager.class);
        consumerOffsetManager = Mockito.mock(ConsumerOffsetManager.class);
        ConsumerOrderInfoManager consumerOrderInfoManager = Mockito.mock(ConsumerOrderInfoManager.class);
        consumerLockService = Mockito.mock(PopConsumerLockService.class);
        messageStore = Mockito.mock(MessageStore.class);

        brokerController = Mockito.mock(BrokerController.class);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        Mockito.when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        Mockito.when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        Mockito.when(brokerController.getConsumerOrderInfoManager()).thenReturn(consumerOrderInfoManager);
        Mockito.when(brokerController.getMessageStore()).thenReturn(messageStore);
        Mockito.when(topicConfigManager.selectTopicConfig(Mockito.anyString()))
            .thenReturn(new TopicConfig(TOPIC_ID));

        consumerService = new PopConsumerService(brokerController);
        // the lock service is built inside the constructor, replace it for verification
        FieldUtils.writeField(consumerService, "consumerLockService", consumerLockService, true);
    }

    @After
    public void shutdown() throws IOException {
        FileUtils.deleteDirectory(new File(filePath));
    }

    private void stubEmptyStore() {
        GetMessageResult result = new GetMessageResult();
        result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        result.setNextBeginOffset(0L);
        Mockito.when(messageStore.getMessageAsync(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyInt(), Mockito.anyLong(), Mockito.anyInt(), Mockito.any()))
            .thenReturn(CompletableFuture.completedFuture(result));
        Mockito.when(consumerOffsetManager.queryOffset(Mockito.anyString(), Mockito.anyString(),
            Mockito.anyInt())).thenReturn(0L);
    }

    @Test
    public void popAsyncOrderlyLockRetryPersistsTest() {
        // the retry keeps spinning until the lock holder releases, no early give-up
        AtomicInteger attempts = new AtomicInteger();
        Mockito.when(consumerLockService.tryLock(Mockito.anyString(), Mockito.anyString()))
            .thenAnswer(invocation -> attempts.incrementAndGet() > 50);
        Mockito.when(subscriptionGroupManager.findSubscriptionGroupConfig(Mockito.anyString()))
            .thenReturn(new SubscriptionGroupConfig());
        stubEmptyStore();

        PopConsumerContext result = consumerService.popAsync("127.0.0.1", System.currentTimeMillis(),
            INVISIBLE_TIME, GROUP_ID, TOPIC_ID, 0, 32, true, ATTEMPT_ID, ConsumeInitMode.MIN, null).join();

        assertNotNull(result);
        Mockito.verify(consumerLockService, Mockito.times(51)).tryLock(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(subscriptionGroupManager).findSubscriptionGroupConfig(GROUP_ID);
    }

    @Test
    public void popAsyncNonFifoFailFastTest() {
        Mockito.when(consumerLockService.tryLock(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(false);

        PopConsumerContext result = consumerService.popAsync("127.0.0.1", System.currentTimeMillis(),
            INVISIBLE_TIME, GROUP_ID, TOPIC_ID, 0, 32, false, null, ConsumeInitMode.MIN, null).join();

        assertNotNull(result);
        assertEquals(0, result.getMessageCount());
        // non-fifo requests must keep the fail-fast behavior
        Mockito.verify(consumerLockService, Mockito.times(1)).tryLock(Mockito.anyString(), Mockito.anyString());
    }
}
