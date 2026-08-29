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
package org.apache.rocketmq.proxy.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ProxyBrokerHeartbeatServiceTest {
    private static final String BROKER_ONE = "127.0.0.1:10911";
    private static final String BROKER_TWO = "127.0.0.2:10911";
    private static final String CLIENT_ID = "ProxyBrokerHeartbeat_test-proxy";
    private static final long INTERVAL_MILLIS = 30_000;
    private static final long TIMEOUT_MILLIS = 3_000;

    @Test
    public void testDisabledServiceDoesNotScheduleOrRun() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        MQClientAPIExt client = clientWithAddresses(BROKER_ONE);
        ProxyBrokerHeartbeatService service = service(
            Collections.singletonList(factoryWithClients(client)), scheduler, false);

        service.start();
        service.runHeartbeatRound();
        service.shutdown();

        assertFalse(service.isStarted());
        assertThat(service.getLastRoundStats().getStartTimestamp()).isZero();
        verify(scheduler, never()).scheduleWithFixedDelay(
            any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        verify(client, never()).sendHeartbeatOneway(anyString(), any(HeartbeatData.class), anyLong());
    }

    @Test
    public void testStartSchedulesHeartbeatWithConfiguredTiming() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        MQClientAPIExt client = clientWithAddresses(BROKER_ONE);
        doReturn(completedHeartbeat()).when(client).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        doReturn(scheduledFuture).when(scheduler).scheduleWithFixedDelay(
            any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        ProxyBrokerHeartbeatService service = service(
            Collections.singletonList(factoryWithClients(client)), scheduler, true);

        service.start();

        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).scheduleWithFixedDelay(taskCaptor.capture(), eq(INTERVAL_MILLIS),
            eq(INTERVAL_MILLIS), eq(TimeUnit.MILLISECONDS));
        assertTrue(service.isStarted());

        taskCaptor.getValue().run();

        verify(client).sendHeartbeatOneway(eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        assertThat(service.getLastRoundStats().getSuccessfulHeartbeatCount()).isOne();
    }

    @Test
    public void testStartAndShutdownAreIdempotent() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(scheduler).scheduleWithFixedDelay(
            any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        ProxyBrokerHeartbeatService service = service(Collections.emptyList(), scheduler, true);

        service.start();
        service.start();

        verify(scheduler, times(1)).scheduleWithFixedDelay(
            any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));

        service.shutdown();
        service.shutdown();

        assertFalse(service.isStarted());
        verify(scheduledFuture, times(1)).cancel(false);
    }

    @Test
    public void testStartResetsStateWhenSchedulerRejectsTask() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        doThrow(new RejectedExecutionException("scheduler stopped")).when(scheduler).scheduleWithFixedDelay(
            any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        ProxyBrokerHeartbeatService service = service(Collections.emptyList(), scheduler, true);

        assertThatThrownBy(service::start).isInstanceOf(RejectedExecutionException.class);

        assertFalse(service.isStarted());
    }

    @Test
    public void testSuccessfulRoundCoversEveryActiveChannel() {
        MQClientAPIExt firstClient = clientWithAddresses(BROKER_ONE, BROKER_TWO);
        MQClientAPIExt secondClient = clientWithAddresses("127.0.0.3:10911");
        doReturn(completedHeartbeat()).when(firstClient).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        doReturn(completedHeartbeat()).when(secondClient).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        ProxyBrokerHeartbeatService service = service(Arrays.asList(
            factoryWithClients(firstClient), factoryWithClients(secondClient)),
            mock(ScheduledExecutorService.class), true);

        service.runHeartbeatRound();

        ArgumentCaptor<String> addressCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HeartbeatData> heartbeatCaptor = ArgumentCaptor.forClass(HeartbeatData.class);
        verify(firstClient, times(2)).sendHeartbeatOneway(addressCaptor.capture(), heartbeatCaptor.capture(),
            eq(TIMEOUT_MILLIS));
        assertThat(addressCaptor.getAllValues()).containsExactlyInAnyOrder(BROKER_ONE, BROKER_TWO);
        assertThat(heartbeatCaptor.getAllValues()).allSatisfy(
            heartbeatData -> assertThat(heartbeatData.getClientID()).isEqualTo(CLIENT_ID));
        verify(secondClient).sendHeartbeatOneway(eq("127.0.0.3:10911"), any(HeartbeatData.class),
            eq(TIMEOUT_MILLIS));

        ProxyBrokerHeartbeatService.HeartbeatRoundStats stats = service.getLastRoundStats();
        assertThat(stats.getClientCount()).isEqualTo(2);
        assertThat(stats.getFailedClientCount()).isZero();
        assertThat(stats.getAttemptedHeartbeatCount()).isEqualTo(3);
        assertThat(stats.getSuccessfulHeartbeatCount()).isEqualTo(3);
        assertThat(stats.getFailedHeartbeatCount()).isZero();
        assertThat(stats.getFinishTimestamp()).isGreaterThanOrEqualTo(stats.getStartTimestamp());
        assertThat(stats.getElapsedMillis()).isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testSameAddressOnSeparateClientsKeepsBothPhysicalChannelsAlive() {
        MQClientAPIExt firstClient = clientWithAddresses(BROKER_ONE);
        MQClientAPIExt secondClient = clientWithAddresses(BROKER_ONE);
        doReturn(completedHeartbeat()).when(firstClient).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        doReturn(completedHeartbeat()).when(secondClient).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        ProxyBrokerHeartbeatService service = service(
            Collections.singletonList(factoryWithClients(firstClient, secondClient)),
            mock(ScheduledExecutorService.class), true);

        service.runHeartbeatRound();

        verify(firstClient).sendHeartbeatOneway(eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        verify(secondClient).sendHeartbeatOneway(eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        assertThat(service.getLastRoundStats().getAttemptedHeartbeatCount()).isEqualTo(2);
    }

    @Test
    public void testHeartbeatFailuresAreCountedAndDoNotStopRound() {
        String exceptionalBroker = "127.0.0.3:10911";
        String nullFutureBroker = "127.0.0.4:10911";
        String throwingBroker = "127.0.0.5:10911";
        MQClientAPIExt client = clientWithAddresses(
            BROKER_ONE, exceptionalBroker, nullFutureBroker, throwingBroker, BROKER_TWO);
        CompletableFuture<Void> failedHeartbeat = new CompletableFuture<>();
        failedHeartbeat.completeExceptionally(new IllegalStateException("send failed"));
        doAnswer(invocation -> {
            String address = invocation.getArgument(0);
            if (exceptionalBroker.equals(address)) {
                return failedHeartbeat;
            }
            if (nullFutureBroker.equals(address)) {
                return null;
            }
            if (throwingBroker.equals(address)) {
                throw new IllegalStateException("invoke failed");
            }
            return completedHeartbeat();
        }).when(client).sendHeartbeatOneway(anyString(), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        ProxyBrokerHeartbeatService service = service(
            Collections.singletonList(factoryWithClients(client)), mock(ScheduledExecutorService.class), true);

        service.runHeartbeatRound();

        verify(client, times(5)).sendHeartbeatOneway(anyString(), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        ProxyBrokerHeartbeatService.HeartbeatRoundStats stats = service.getLastRoundStats();
        assertThat(stats.getAttemptedHeartbeatCount()).isEqualTo(5);
        assertThat(stats.getSuccessfulHeartbeatCount()).isEqualTo(2);
        assertThat(stats.getFailedHeartbeatCount()).isEqualTo(3);
    }

    @Test
    public void testClientDiscoveryFailureDoesNotBlockOtherClients() {
        MQClientAPIExt failedClient = mock(MQClientAPIExt.class);
        doThrow(new IllegalStateException("channel table unavailable"))
            .when(failedClient).getActiveBrokerAddresses();
        MQClientAPIExt healthyClient = clientWithAddresses(BROKER_ONE);
        doReturn(completedHeartbeat()).when(healthyClient).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        ProxyBrokerHeartbeatService service = service(
            Collections.singletonList(factoryWithClients(failedClient, healthyClient)),
            mock(ScheduledExecutorService.class), true);

        service.runHeartbeatRound();

        verify(healthyClient).sendHeartbeatOneway(eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        ProxyBrokerHeartbeatService.HeartbeatRoundStats stats = service.getLastRoundStats();
        assertThat(stats.getClientCount()).isEqualTo(2);
        assertThat(stats.getFailedClientCount()).isOne();
        assertThat(stats.getSuccessfulHeartbeatCount()).isOne();
    }

    @Test
    public void testFactoryFailureDoesNotBlockOtherFactories() {
        MQClientAPIFactory failedFactory = mock(MQClientAPIFactory.class);
        doThrow(new IllegalStateException("factory unavailable")).when(failedFactory).getClients();
        MQClientAPIExt healthyClient = clientWithAddresses(BROKER_ONE);
        doReturn(completedHeartbeat()).when(healthyClient).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        ProxyBrokerHeartbeatService service = service(
            Arrays.asList(failedFactory, factoryWithClients(healthyClient)),
            mock(ScheduledExecutorService.class), true);

        service.runHeartbeatRound();

        verify(healthyClient).sendHeartbeatOneway(eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        ProxyBrokerHeartbeatService.HeartbeatRoundStats stats = service.getLastRoundStats();
        assertThat(stats.getClientCount()).isOne();
        assertThat(stats.getFailedClientCount()).isOne();
        assertThat(stats.getSuccessfulHeartbeatCount()).isOne();
    }

    @Test
    public void testNullAndBlankDiscoveryEntriesAreIgnored() {
        MQClientAPIFactory nullClientsFactory = mock(MQClientAPIFactory.class);
        doReturn(null).when(nullClientsFactory).getClients();
        MQClientAPIExt client = clientWithAddresses(null, "", "   ", BROKER_ONE);
        doReturn(completedHeartbeat()).when(client).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        ProxyBrokerHeartbeatService service = service(
            Arrays.asList(null, nullClientsFactory, factoryWithClients(null, client)),
            mock(ScheduledExecutorService.class), true);

        service.runHeartbeatRound();

        verify(client, times(1)).sendHeartbeatOneway(
            eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
        assertThat(service.getLastRoundStats().getClientCount()).isOne();
        assertThat(service.getLastRoundStats().getAttemptedHeartbeatCount()).isOne();
    }

    @Test
    public void testOverlappingRoundIsSkipped() throws Exception {
        CountDownLatch discoveryStarted = new CountDownLatch(1);
        CountDownLatch continueDiscovery = new CountDownLatch(1);
        MQClientAPIExt slowClient = mock(MQClientAPIExt.class);
        doAnswer(invocation -> {
            discoveryStarted.countDown();
            if (!continueDiscovery.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test timed out waiting to continue discovery");
            }
            return Collections.singleton(BROKER_ONE);
        }).when(slowClient).getActiveBrokerAddresses();
        doReturn(completedHeartbeat()).when(slowClient).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        ProxyBrokerHeartbeatService service = service(
            Collections.singletonList(factoryWithClients(slowClient)),
            mock(ScheduledExecutorService.class), true);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            Future<?> firstRound = executorService.submit(service::runHeartbeatRound);
            assertTrue(discoveryStarted.await(5, TimeUnit.SECONDS));
            assertTrue(service.isRoundRunning());

            service.runHeartbeatRound();

            verify(slowClient, times(1)).getActiveBrokerAddresses();
            continueDiscovery.countDown();
            firstRound.get(5, TimeUnit.SECONDS);
        } finally {
            continueDiscovery.countDown();
            executorService.shutdownNow();
        }

        assertFalse(service.isRoundRunning());
        verify(slowClient, times(1)).sendHeartbeatOneway(
            eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
    }

    @Test
    public void testSafeRunnerContainsUnexpectedFailure() {
        ProxyBrokerHeartbeatService service = new ProxyBrokerHeartbeatService(
            Collections.emptyList(), mock(ScheduledExecutorService.class), true,
            INTERVAL_MILLIS, TIMEOUT_MILLIS, CLIENT_ID) {
            @Override
            void runHeartbeatRound() {
                throw new AssertionError("unexpected failure");
            }
        };

        service.runHeartbeatRoundSafely();
    }

    @Test
    public void testPublicConstructorUsesConfiguredProxyIdentity() {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setProxyName("proxy-a");
        proxyConfig.setEnableProxyBrokerHeartbeat(true);
        proxyConfig.setProxyBrokerHeartbeatIntervalMillis(45_000);
        proxyConfig.setProxyBrokerHeartbeatTimeoutMillis(5_000);
        MQClientAPIExt client = clientWithAddresses(BROKER_ONE);
        doReturn(completedHeartbeat()).when(client).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        ProxyBrokerHeartbeatService service = new ProxyBrokerHeartbeatService(
            Collections.singletonList(factoryWithClients(client)),
            mock(ScheduledExecutorService.class), proxyConfig);

        service.runHeartbeatRound();

        ArgumentCaptor<HeartbeatData> heartbeatCaptor = ArgumentCaptor.forClass(HeartbeatData.class);
        verify(client).sendHeartbeatOneway(eq(BROKER_ONE), heartbeatCaptor.capture(), eq(5_000L));
        assertThat(heartbeatCaptor.getValue().getClientID()).isEqualTo("ProxyBrokerHeartbeat_proxy-a");
    }

    @Test
    public void testConstructorTakesDefensiveFactorySnapshot() {
        MQClientAPIExt client = clientWithAddresses(BROKER_ONE);
        doReturn(completedHeartbeat()).when(client).sendHeartbeatOneway(
            anyString(), any(HeartbeatData.class), anyLong());
        MQClientAPIFactory factory = factoryWithClients(client);
        List<MQClientAPIFactory> mutableFactories = new ArrayList<>();
        mutableFactories.add(factory);
        ProxyBrokerHeartbeatService service = service(
            mutableFactories, mock(ScheduledExecutorService.class), true);

        mutableFactories.clear();
        service.runHeartbeatRound();

        verify(factory).getClients();
        verify(client).sendHeartbeatOneway(eq(BROKER_ONE), any(HeartbeatData.class), eq(TIMEOUT_MILLIS));
    }

    @Test
    public void testRoundStatsDescribeFailuresForOperationalLogs() {
        ProxyBrokerHeartbeatService.HeartbeatRoundStats stats =
            new ProxyBrokerHeartbeatService.HeartbeatRoundStats(100, 125, 4, 1, 3, 2, 1);

        assertThat(stats.getElapsedMillis()).isEqualTo(25);
        assertThat(stats.toString())
            .contains("elapsedMillis=25")
            .contains("clientCount=4")
            .contains("failedClientCount=1")
            .contains("attemptedHeartbeatCount=3")
            .contains("successfulHeartbeatCount=2")
            .contains("failedHeartbeatCount=1");
    }

    private ProxyBrokerHeartbeatService service(List<MQClientAPIFactory> factories,
        ScheduledExecutorService scheduler, boolean enabled) {
        return new ProxyBrokerHeartbeatService(factories, scheduler, enabled,
            INTERVAL_MILLIS, TIMEOUT_MILLIS, CLIENT_ID);
    }

    private MQClientAPIFactory factoryWithClients(MQClientAPIExt... clients) {
        MQClientAPIFactory factory = mock(MQClientAPIFactory.class);
        doReturn(clients).when(factory).getClients();
        return factory;
    }

    private MQClientAPIExt clientWithAddresses(String... addresses) {
        MQClientAPIExt client = mock(MQClientAPIExt.class);
        Set<String> addressSet = new LinkedHashSet<>(Arrays.asList(addresses));
        doReturn(addressSet).when(client).getActiveBrokerAddresses();
        return client;
    }

    private CompletableFuture<Void> completedHeartbeat() {
        return CompletableFuture.completedFuture(null);
    }
}
