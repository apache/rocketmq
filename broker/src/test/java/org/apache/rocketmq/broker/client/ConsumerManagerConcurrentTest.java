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

package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Test concurrent registration to verify thread safety of ConsumerManager.
 * This test ensures that the fix for the concurrent HashSet modification bug works correctly.
 */
public class ConsumerManagerConcurrentTest {

    private ConsumerManager consumerManager;
    private final BrokerConfig brokerConfig = new BrokerConfig();

    @Before
    public void before() {
        DefaultConsumerIdsChangeListener defaultConsumerIdsChangeListener =
            new DefaultConsumerIdsChangeListener(null);
        BrokerStatsManager brokerStatsManager = new BrokerStatsManager(brokerConfig);
        consumerManager = new ConsumerManager(defaultConsumerIdsChangeListener, brokerStatsManager, brokerConfig);
    }

    /**
     * Test concurrent consumer registration for the same topic.
     * This test verifies that no data is lost when multiple threads register consumers concurrently.
     *
     * Before fix: Using HashSet in topicGroupTable could cause data loss (60% reproduction rate)
     * After fix: Using ConcurrentHashMap.newKeySet() ensures thread safety
     */
    @Test
    public void testConcurrentRegisterConsumer() throws InterruptedException {
        int threadCount = 100;
        String topic = "TestTopic";

        ExecutorService executor = Executors.newFixedThreadPool(50);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    String group = "Group_" + index;
                    Channel channel = mock(Channel.class);
                    ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                        channel, "Client_" + index, LanguageCode.JAVA, 1);

                    Set<SubscriptionData> subList = new HashSet<>();
                    subList.add(new SubscriptionData(topic, "*"));

                    boolean registered = consumerManager.registerConsumer(
                        group,
                        clientChannelInfo,
                        ConsumeType.CONSUME_PASSIVELY,
                        MessageModel.CLUSTERING,
                        ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
                        subList,
                        false
                    );

                    if (registered) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads at the same time to maximize contention
        startLatch.countDown();

        // Wait for all threads to complete
        boolean finished = endLatch.await(10, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        executor.shutdown();

        // Verify the result
        HashSet<String> groups = consumerManager.queryTopicConsumeByWho(topic);

        // After fix, we should have exactly threadCount groups (no data loss)
        assertThat(groups.size()).isEqualTo(threadCount);
        assertThat(successCount.get()).isEqualTo(threadCount);
    }

    /**
     * Test concurrent registration with multiple runs to ensure consistency.
     */
    @Test
    public void testConcurrentRegisterConsistency() throws InterruptedException {
        int iterations = 10;
        int threadCount = 50;

        for (int iter = 0; iter < iterations; iter++) {
            final int iteration = iter;
            String topic = "Topic_" + iteration;

            ExecutorService executor = Executors.newFixedThreadPool(30);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                final int index = i;
                final String topicFinal = topic;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        String group = "Group_" + iteration + "_" + index;
                        Channel channel = mock(Channel.class);
                        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                            channel, "Client_" + index, LanguageCode.JAVA, 1);

                        Set<SubscriptionData> subList = new HashSet<>();
                        subList.add(new SubscriptionData(topicFinal, "*"));

                        consumerManager.registerConsumer(
                            group,
                            clientChannelInfo,
                            ConsumeType.CONSUME_PASSIVELY,
                            MessageModel.CLUSTERING,
                            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
                            subList,
                            false
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            boolean finished = endLatch.await(5, TimeUnit.SECONDS);
            assertThat(finished).isTrue();
            executor.shutdown();

            // Verify no data loss in each iteration
            HashSet<String> groups = consumerManager.queryTopicConsumeByWho(topic);
            assertThat(groups.size()).isEqualTo(threadCount);
        }
    }

    /**
     * Test high stress scenario with more threads.
     */
    @Test
    public void testHighConcurrencyStress() throws InterruptedException {
        int threadCount = 200;
        String topic = "StressTestTopic";

        ExecutorService executor = Executors.newFixedThreadPool(100);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    String group = "StressGroup_" + index;
                    Channel channel = mock(Channel.class);
                    ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                        channel, "StressClient_" + index, LanguageCode.JAVA, 1);

                    Set<SubscriptionData> subList = new HashSet<>();
                    subList.add(new SubscriptionData(topic, "*"));

                    consumerManager.registerConsumer(
                        group,
                        clientChannelInfo,
                        ConsumeType.CONSUME_PASSIVELY,
                        MessageModel.CLUSTERING,
                        ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
                        subList,
                        false
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = endLatch.await(15, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        executor.shutdown();

        // Verify no data loss under high stress
        HashSet<String> groups = consumerManager.queryTopicConsumeByWho(topic);
        assertThat(groups.size()).isEqualTo(threadCount);
    }

    /**
     * Test concurrent registration for multiple topics.
     */
    @Test
    public void testConcurrentRegisterMultipleTopics() throws InterruptedException {
        int threadCount = 50;
        int topicCount = 10;

        ExecutorService executor = Executors.newFixedThreadPool(50);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount * topicCount);

        for (int t = 0; t < topicCount; t++) {
            final String topic = "MultiTopic_" + t;
            for (int i = 0; i < threadCount; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        String group = "MultiGroup_" + topic + "_" + index;
                        Channel channel = mock(Channel.class);
                        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                            channel, "MultiClient_" + index, LanguageCode.JAVA, 1);

                        Set<SubscriptionData> subList = new HashSet<>();
                        subList.add(new SubscriptionData(topic, "*"));

                        consumerManager.registerConsumer(
                            group,
                            clientChannelInfo,
                            ConsumeType.CONSUME_PASSIVELY,
                            MessageModel.CLUSTERING,
                            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
                            subList,
                            false
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endLatch.countDown();
                    }
                });
            }
        }

        startLatch.countDown();
        boolean finished = endLatch.await(15, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        executor.shutdown();

        // Verify each topic has exactly threadCount groups
        for (int t = 0; t < topicCount; t++) {
            String topic = "MultiTopic_" + t;
            HashSet<String> groups = consumerManager.queryTopicConsumeByWho(topic);
            assertThat(groups.size()).isEqualTo(threadCount);
        }
    }
}
