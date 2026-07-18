/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.admin.client;

import apache.rocketmq.v2.ClientType;
import java.util.Collections;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientReadServiceCleanerTest {

    @Test
    public void cleanupRemovesClientsAtOrBeforeTimeoutCutoff() {
        ProxyClientReadService readService = new ProxyClientReadService();
        readService.upsertClient(client("client-old", 400L));
        readService.upsertClient(client("client-boundary", 500L));
        readService.upsertClient(client("client-active", 501L));
        AtomicLong now = new AtomicLong(1000L);
        ProxyClientReadServiceCleaner cleaner = new ProxyClientReadServiceCleaner(
            readService,
            500L,
            1000L,
            new RecordingScheduledExecutor(),
            now::get
        );

        int removed = cleaner.cleanup();

        assertThat(removed).isEqualTo(2);
        assertThat(readService.getClient("client-old")).isNull();
        assertThat(readService.getClient("client-boundary")).isNull();
        assertThat(readService.getClient("client-active")).isNotNull();
    }

    @Test
    public void startSchedulesCleanupWithConfiguredInterval() throws Exception {
        ProxyClientReadService readService = new ProxyClientReadService();
        readService.upsertClient(client("client-old", 400L));
        AtomicLong now = new AtomicLong(1000L);
        RecordingScheduledExecutor executor = new RecordingScheduledExecutor();
        ProxyClientReadServiceCleaner cleaner = new ProxyClientReadServiceCleaner(
            readService,
            500L,
            1000L,
            executor,
            now::get
        );

        cleaner.start();

        assertThat(executor.command).isNotNull();
        assertThat(executor.initialDelay).isEqualTo(1000L);
        assertThat(executor.delay).isEqualTo(1000L);
        assertThat(executor.unit).isEqualTo(TimeUnit.MILLISECONDS);

        executor.command.run();

        assertThat(readService.getClient("client-old")).isNull();
    }

    @Test
    public void shutdownStopsScheduledExecutor() throws Exception {
        RecordingScheduledExecutor executor = new RecordingScheduledExecutor();
        ProxyClientReadServiceCleaner cleaner = new ProxyClientReadServiceCleaner(
            new ProxyClientReadService(),
            500L,
            1000L,
            executor,
            System::currentTimeMillis
        );

        cleaner.shutdown();

        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    public void constructorRejectsInvalidArguments() {
        ScheduledExecutorService executor = new RecordingScheduledExecutor();

        assertThatThrownBy(() -> new ProxyClientReadServiceCleaner(
            null,
            500L,
            1000L,
            executor,
            System::currentTimeMillis
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyClientReadService is required");
        assertThatThrownBy(() -> new ProxyClientReadServiceCleaner(
            new ProxyClientReadService(),
            0L,
            1000L,
            executor,
            System::currentTimeMillis
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("inactiveTimeoutMillis must be positive");
        assertThatThrownBy(() -> new ProxyClientReadServiceCleaner(
            new ProxyClientReadService(),
            500L,
            0L,
            executor,
            System::currentTimeMillis
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cleanupIntervalMillis must be positive");
        assertThatThrownBy(() -> new ProxyClientReadServiceCleaner(
            new ProxyClientReadService(),
            500L,
            1000L,
            null,
            System::currentTimeMillis
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("scheduledExecutorService is required");
        assertThatThrownBy(() -> new ProxyClientReadServiceCleaner(
            new ProxyClientReadService(),
            500L,
            1000L,
            executor,
            null
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("currentTimeMillisSupplier is required");
    }

    private static ProxyClientInfo client(String clientId, long lastActiveTimeMillis) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.singleton("group-a"),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            "proxy-a",
            100L,
            lastActiveTimeMillis
        );
    }

    private static class RecordingScheduledExecutor extends ScheduledThreadPoolExecutor {
        private Runnable command;
        private long initialDelay;
        private long delay;
        private TimeUnit unit;

        private RecordingScheduledExecutor() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
            TimeUnit unit) {
            this.command = command;
            this.initialDelay = initialDelay;
            this.delay = delay;
            this.unit = unit;
            return new CompletedScheduledFuture();
        }
    }

    private static class CompletedScheduledFuture implements ScheduledFuture<Object> {
        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed other) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }
}
