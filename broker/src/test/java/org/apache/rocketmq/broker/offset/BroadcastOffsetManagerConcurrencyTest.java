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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BroadcastOffsetManagerConcurrencyTest {

    @Test
    public void testQueryInitOffsetDoesNotOverwriteConcurrentUpdate() throws Exception {
        String topic = "topic";
        String group = "group";
        String clientId = "client";
        long updatedOffset = 100L;
        BrokerController brokerController = mock(BrokerController.class);
        when(brokerController.getBrokerConfig()).thenReturn(mock(BrokerConfig.class));
        ConsumerOffsetManager consumerOffsetManager = mock(ConsumerOffsetManager.class);
        when(consumerOffsetManager.queryOffset(anyString(), anyString(), anyInt())).thenReturn(-1L);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getMaxOffsetInQueue(anyString(), anyInt(), anyBoolean())).thenReturn(10L);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        BroadcastOffsetManager broadcastOffsetManager = new BroadcastOffsetManager(brokerController);

        BroadcastOffsetManager.BroadcastOffsetData offsetData =
            new BroadcastOffsetManager.BroadcastOffsetData(topic, group);
        BlockingInitOffsetStore clientOffsetStore = new BlockingInitOffsetStore();
        FieldUtils.writeDeclaredField(offsetData, "clientOffsetStore", clientOffsetStore, true);
        broadcastOffsetManager.offsetStoreMap.put(topic + "@" + group, offsetData);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Long> queryFuture = executor.submit(() -> broadcastOffsetManager.queryInitOffset(
                topic, group, 0, clientId, -1L, true));

            Assert.assertTrue("Query did not start client offset initialization",
                clientOffsetStore.awaitInitialization());
            broadcastOffsetManager.updateOffset(topic, group, 0, updatedOffset, clientId, true);
            clientOffsetStore.continueInitialization();

            Assert.assertEquals(updatedOffset, queryFuture.get(5, TimeUnit.SECONDS).longValue());
            Assert.assertEquals(updatedOffset,
                broadcastOffsetManager.queryInitOffset(topic, group, 0, clientId, -1L, true).longValue());
        } finally {
            clientOffsetStore.continueInitialization();
            executor.shutdownNow();
        }
    }

    private static class BlockingInitOffsetStore
        extends ConcurrentHashMap<String, BroadcastOffsetManager.BroadcastTimedOffsetStore> {
        private final CountDownLatch initializationStarted = new CountDownLatch(1);
        private final CountDownLatch continueInitialization = new CountDownLatch(1);

        @Override
        public BroadcastOffsetManager.BroadcastTimedOffsetStore put(String key,
            BroadcastOffsetManager.BroadcastTimedOffsetStore value) {
            blockInitialization();
            return super.put(key, value);
        }

        @Override
        public BroadcastOffsetManager.BroadcastTimedOffsetStore computeIfAbsent(String key,
            Function<? super String, ? extends BroadcastOffsetManager.BroadcastTimedOffsetStore> mappingFunction) {
            BroadcastOffsetManager.BroadcastTimedOffsetStore offsetStore = super.computeIfAbsent(key, mappingFunction);
            blockInitialization();
            return offsetStore;
        }

        private boolean awaitInitialization() throws InterruptedException {
            return initializationStarted.await(5, TimeUnit.SECONDS);
        }

        private void continueInitialization() {
            continueInitialization.countDown();
        }

        private void blockInitialization() {
            initializationStarted.countDown();
            try {
                if (!continueInitialization.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting to continue offset-store initialization");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting to continue offset-store initialization", e);
            }
        }
    }
}
