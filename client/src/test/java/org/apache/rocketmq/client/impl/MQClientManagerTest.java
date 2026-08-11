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
package org.apache.rocketmq.client.impl;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.common.ServiceState;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MQClientManagerTest {
    private static final String SCHEDULER_THREAD_NAME = "MQClientFactoryScheduledThread";
    private static final long TIMEOUT_SECONDS = 30;

    private final Set<MQClientInstance> instancesToDispose =
        Collections.newSetFromMap(new IdentityHashMap<MQClientInstance, Boolean>());

    @After
    public void tearDown() throws Exception {
        for (MQClientInstance instance : instancesToDispose) {
            dispose(instance);
        }
        instancesToDispose.clear();
    }

    @Test
    public void concurrentSameClientIdCreatesOneInstance() throws Exception {
        int callers = 16;
        MQClientManager manager = newManager();
        FieldUtils.writeDeclaredField(manager, "factoryTable", new BarrierGetMap<>(callers), true);
        ThreadGroup threadGroup = new ThreadGroup("same-client-id-" + System.nanoTime());
        ExecutorService executor = newExecutor(threadGroup, callers);
        ClientConfig config = newConfig("same-client-id");

        try {
            List<Future<MQClientInstance>> futures = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                futures.add(executor.submit(() -> manager.getOrCreateMQClientInstance(config)));
            }

            Set<MQClientInstance> returned = Collections.newSetFromMap(
                new IdentityHashMap<MQClientInstance, Boolean>());
            for (Future<MQClientInstance> future : futures) {
                returned.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertThat(returned).hasSize(1);
            MQClientInstance instance = returned.iterator().next();
            track(instance);
            assertSame(instance, manager.getFactoryTable().get(config.buildMQClientId()));
            assertThat(factoryIndex(manager).get()).isEqualTo(1);
            awaitSchedulerThreadCount(threadGroup, 1);
        } finally {
            shutdown(executor);
        }
    }

    @Test
    public void differentClientIdsAreConstructedConcurrently() throws Exception {
        MQClientManager manager = newManager();
        CyclicBarrier constructorBarrier = new CyclicBarrier(2);
        ThreadGroup threadGroup = new ThreadGroup("different-client-id-" + System.nanoTime());
        ExecutorService executor = newExecutor(threadGroup, 2);
        ClientConfig firstConfig = concurrentConstructorConfig(
            "different-a-" + System.nanoTime(), constructorBarrier);
        ClientConfig secondConfig = concurrentConstructorConfig(
            differentHashBinInstanceName(firstConfig.buildMQClientId(), "different-b"), constructorBarrier);

        try {
            Future<MQClientInstance> firstFuture = executor.submit(
                () -> manager.getOrCreateMQClientInstance(firstConfig));
            Future<MQClientInstance> secondFuture = executor.submit(
                () -> manager.getOrCreateMQClientInstance(secondConfig));

            MQClientInstance first = firstFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            MQClientInstance second = secondFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            track(first);
            track(second);
            assertThat(first).isNotSameAs(second);
            assertThat(manager.getFactoryTable()).hasSize(2);
            assertThat(factoryIndex(manager).get()).isEqualTo(2);
            awaitSchedulerThreadCount(threadGroup, 2);
        } finally {
            shutdown(executor);
        }
    }

    @Test
    public void constructorFailureRollsBackResourcesAndAllowsRetry() throws Exception {
        MQClientManager manager = newManager();
        ThreadGroup threadGroup = new ThreadGroup("constructor-failure-" + System.nanoTime());
        ExecutorService executor = newExecutor(threadGroup, 1);
        ClientConfig config = newConfig("constructor-failure");
        config.setEnableConcurrentHeartbeat(true);
        config.setConcurrentHeartbeatThreadPoolSize(0);

        try {
            Future<MQClientInstance> failed = executor.submit(
                () -> manager.getOrCreateMQClientInstance(config));
            try {
                failed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                fail("Expected constructor failure");
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
            }

            assertThat(manager.getFactoryTable()).doesNotContainKey(config.buildMQClientId());
            awaitSchedulerThreadCount(threadGroup, 0);

            config.setConcurrentHeartbeatThreadPoolSize(1);
            MQClientInstance retried = executor.submit(
                () -> manager.getOrCreateMQClientInstance(config)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            track(retried);
            assertSame(retried, manager.getFactoryTable().get(config.buildMQClientId()));
            awaitSchedulerThreadCount(threadGroup, 1);
        } finally {
            shutdown(executor);
        }
    }

    @Test
    public void removeAllowsInstanceToBeRecreated() throws Exception {
        MQClientManager manager = newManager();
        ClientConfig config = newConfig("remove-recreate");
        MQClientInstance first = manager.getOrCreateMQClientInstance(config);
        track(first);

        manager.removeClientFactory(config.buildMQClientId());
        MQClientInstance second = manager.getOrCreateMQClientInstance(config);
        track(second);

        assertThat(second).isNotSameAs(first);
        assertSame(second, manager.getFactoryTable().get(config.buildMQClientId()));
        manager.removeClientFactory(config.buildMQClientId(), first);
        assertSame(second, manager.getFactoryTable().get(config.buildMQClientId()));
    }

    @Test
    public void startAndShutdownRemainIdempotentAndAllowRecreation() throws Exception {
        MQClientManager manager = MQClientManager.getInstance();
        ClientConfig config = newConfig("start-shutdown");
        ThreadGroup threadGroup = new ThreadGroup("start-shutdown-" + System.nanoTime());
        ExecutorService executor = newExecutor(threadGroup, 1);

        try {
            MQClientInstance first = executor.submit(
                () -> manager.getOrCreateMQClientInstance(config)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            track(first);
            first.start();
            first.start();
            assertThat(serviceState(first)).isEqualTo(ServiceState.RUNNING);

            first.shutdown();
            first.shutdown();
            assertThat(serviceState(first)).isEqualTo(ServiceState.SHUTDOWN_ALREADY);
            assertThat(manager.getFactoryTable()).doesNotContainKey(config.buildMQClientId());
            assertExecutorTerminated(scheduler(first));

            MQClientInstance second = executor.submit(
                () -> manager.getOrCreateMQClientInstance(config)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            track(second);
            assertThat(second).isNotSameAs(first);
            assertSame(second, manager.getFactoryTable().get(config.buildMQClientId()));
        } finally {
            shutdown(executor);
        }
    }

    @Test
    public void startFailureRollsBackResourcesAndAllowsReplacement() throws Exception {
        MQClientManager manager = MQClientManager.getInstance();
        ClientConfig config = newConfig("start-failure");
        ThreadGroup threadGroup = new ThreadGroup("start-failure-" + System.nanoTime());
        ExecutorService executor = newExecutor(threadGroup, 1);

        try {
            MQClientInstance failedInstance = executor.submit(
                () -> manager.getOrCreateMQClientInstance(config)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            track(failedInstance);
            MQClientAPIImpl originalClientAPI = clientAPI(failedInstance);
            originalClientAPI.shutdown();
            PullMessageService originalPullMessageService = (PullMessageService) FieldUtils.readDeclaredField(
                failedInstance, "pullMessageService", true);
            originalPullMessageService.shutdown(true);
            MQClientAPIImpl clientAPI = mock(MQClientAPIImpl.class);
            PullMessageService failingPullMessageService = mock(PullMessageService.class);
            doThrow(new IllegalStateException("injected start failure")).when(failingPullMessageService).start();
            FieldUtils.writeDeclaredField(failedInstance, "mQClientAPIImpl", clientAPI, true);
            FieldUtils.writeDeclaredField(failedInstance, "pullMessageService", failingPullMessageService, true);

            assertThrows(IllegalStateException.class, failedInstance::start);
            verify(clientAPI).start();
            verify(clientAPI).shutdown();
            verify(failingPullMessageService).start();
            verify(failingPullMessageService).shutdown(true);
            assertThat(serviceState(failedInstance)).isEqualTo(ServiceState.START_FAILED);
            assertThat(manager.getFactoryTable()).doesNotContainKey(config.buildMQClientId());
            assertExecutorTerminated(scheduler(failedInstance));
            awaitSchedulerThreadCount(threadGroup, 0);
            assertThrows(MQClientException.class, failedInstance::start);

            MQClientInstance replacement = executor.submit(
                () -> manager.getOrCreateMQClientInstance(config)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            track(replacement);
            assertThat(replacement).isNotSameAs(failedInstance);
            assertSame(replacement, manager.getFactoryTable().get(config.buildMQClientId()));
        } finally {
            shutdown(executor);
        }
    }

    @Test
    public void staleShutdownDoesNotRemoveReplacement() throws Exception {
        MQClientManager manager = MQClientManager.getInstance();
        ClientConfig config = newConfig("stale-shutdown");
        MQClientInstance first = manager.getOrCreateMQClientInstance(config);
        track(first);
        first.start();

        manager.removeClientFactory(config.buildMQClientId());
        MQClientInstance replacement = manager.getOrCreateMQClientInstance(config);
        track(replacement);
        first.shutdown();

        assertSame(replacement, manager.getFactoryTable().get(config.buildMQClientId()));
    }

    private MQClientInstance track(MQClientInstance instance) {
        instancesToDispose.add(instance);
        return instance;
    }

    private static MQClientManager newManager() throws Exception {
        Constructor<MQClientManager> constructor = MQClientManager.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    private static ClientConfig newConfig(String suffix) {
        ClientConfig config = new ClientConfig();
        config.setInstanceName(suffix + "-" + System.nanoTime());
        config.setNamesrvAddr("127.0.0.1:9876");
        return config;
    }

    private static ClientConfig concurrentConstructorConfig(String instanceName, CyclicBarrier barrier) {
        ClientConfig config = new ClientConfig() {
            @Override
            public ClientConfig cloneClientConfig() {
                ClientConfig cloned = new ClientConfig() {
                    @Override
                    public int getClientCallbackExecutorThreads() {
                        await(barrier);
                        return super.getClientCallbackExecutorThreads();
                    }
                };
                cloned.resetClientConfig(this);
                return cloned;
            }
        };
        config.setInstanceName(instanceName);
        config.setNamesrvAddr("127.0.0.1:9876");
        return config;
    }

    private static String differentHashBinInstanceName(String firstClientId, String prefix) {
        int firstBin = spread(firstClientId.hashCode()) & 15;
        for (int i = 0; ; i++) {
            String candidate = prefix + "-" + i;
            ClientConfig config = new ClientConfig();
            config.setInstanceName(candidate);
            if ((spread(config.buildMQClientId().hashCode()) & 15) != firstBin) {
                return candidate;
            }
        }
    }

    private static int spread(int hashCode) {
        return hashCode ^ (hashCode >>> 16);
    }

    private static AtomicInteger factoryIndex(MQClientManager manager) throws IllegalAccessException {
        return (AtomicInteger) FieldUtils.readDeclaredField(manager, "factoryIndexGenerator", true);
    }

    private static ServiceState serviceState(MQClientInstance instance) throws IllegalAccessException {
        return (ServiceState) FieldUtils.readDeclaredField(instance, "serviceState", true);
    }

    private static ScheduledExecutorService scheduler(MQClientInstance instance) throws IllegalAccessException {
        return (ScheduledExecutorService) FieldUtils.readDeclaredField(
            instance, "scheduledExecutorService", true);
    }

    private static MQClientAPIImpl clientAPI(MQClientInstance instance) throws IllegalAccessException {
        return (MQClientAPIImpl) FieldUtils.readDeclaredField(instance, "mQClientAPIImpl", true);
    }

    private static ExecutorService newExecutor(ThreadGroup threadGroup, int threads) {
        AtomicInteger index = new AtomicInteger();
        return Executors.newFixedThreadPool(threads,
            task -> new Thread(threadGroup, task, "MQClientManagerTestCaller-" + index.getAndIncrement()));
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    private static void awaitSchedulerThreadCount(ThreadGroup threadGroup, int expected) {
        org.awaitility.Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() ->
            assertThat(countThreads(threadGroup, SCHEDULER_THREAD_NAME)).isEqualTo(expected));
    }

    private static int countThreads(ThreadGroup threadGroup, String threadName) {
        int capacity = Math.max(16, threadGroup.activeCount() * 2);
        while (true) {
            Thread[] threads = new Thread[capacity];
            int count = threadGroup.enumerate(threads, true);
            if (count < capacity) {
                int matches = 0;
                for (int i = 0; i < count; i++) {
                    if (threadName.equals(threads[i].getName()) && threads[i].isAlive()) {
                        matches++;
                    }
                }
                return matches;
            }
            capacity *= 2;
        }
    }

    private static void assertExecutorTerminated(ScheduledExecutorService executor) throws InterruptedException {
        assertThat(executor.isShutdown()).isTrue();
        assertThat(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    private static void dispose(MQClientInstance instance) throws Exception {
        try {
            instance.shutdown();
        } finally {
            ScheduledExecutorService scheduler = scheduler(instance);
            scheduler.shutdownNow();
            scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            ExecutorService heartbeatExecutor = (ExecutorService) FieldUtils.readDeclaredField(
                instance, "concurrentHeartbeatExecutor", true);
            if (heartbeatExecutor != null) {
                heartbeatExecutor.shutdownNow();
                heartbeatExecutor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            DefaultMQProducerImpl producer = (DefaultMQProducerImpl) FieldUtils.readDeclaredField(
                FieldUtils.readDeclaredField(instance, "defaultMQProducer", true),
                "defaultMQProducerImpl", true);
            producer.shutdown(false);
            ((PullMessageService) FieldUtils.readDeclaredField(instance, "pullMessageService", true)).shutdown(true);
            clientAPI(instance).shutdown();
            MQClientManager.getInstance().removeClientFactory(instance.getClientId(), instance);
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (BrokenBarrierException | TimeoutException e) {
            throw new AssertionError(e);
        }
    }

    private static class BarrierGetMap<K, V> extends ConcurrentHashMap<K, V> {
        private final CyclicBarrier barrier;

        BarrierGetMap(int parties) {
            this.barrier = new CyclicBarrier(parties);
        }

        @Override
        public V get(Object key) {
            V value = super.get(key);
            if (value == null) {
                await(barrier);
            }
            return value;
        }
    }
}
