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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ProxyClientReadServiceCleaner implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ProxyClientReadService proxyClientReadService;
    private final long inactiveTimeoutMillis;
    private final long cleanupIntervalMillis;
    private final ScheduledExecutorService scheduledExecutorService;
    private final LongSupplier currentTimeMillisSupplier;

    public ProxyClientReadServiceCleaner(ProxyClientReadService proxyClientReadService, long inactiveTimeoutMillis,
        long cleanupIntervalMillis, ScheduledExecutorService scheduledExecutorService,
        LongSupplier currentTimeMillisSupplier) {
        if (proxyClientReadService == null) {
            throw new IllegalArgumentException("proxyClientReadService is required");
        }
        if (inactiveTimeoutMillis <= 0) {
            throw new IllegalArgumentException("inactiveTimeoutMillis must be positive");
        }
        if (cleanupIntervalMillis <= 0) {
            throw new IllegalArgumentException("cleanupIntervalMillis must be positive");
        }
        if (scheduledExecutorService == null) {
            throw new IllegalArgumentException("scheduledExecutorService is required");
        }
        if (currentTimeMillisSupplier == null) {
            throw new IllegalArgumentException("currentTimeMillisSupplier is required");
        }
        this.proxyClientReadService = proxyClientReadService;
        this.inactiveTimeoutMillis = inactiveTimeoutMillis;
        this.cleanupIntervalMillis = cleanupIntervalMillis;
        this.scheduledExecutorService = scheduledExecutorService;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
    }

    public int cleanup() {
        long cutoffLastActiveTimeMillis = this.currentTimeMillisSupplier.getAsLong() - this.inactiveTimeoutMillis;
        return this.proxyClientReadService.removeInactiveClients(cutoffLastActiveTimeMillis);
    }

    @Override
    public void start() {
        this.scheduledExecutorService.scheduleWithFixedDelay(
            this::safeCleanup,
            this.cleanupIntervalMillis,
            this.cleanupIntervalMillis,
            TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    private void safeCleanup() {
        try {
            int removed = this.cleanup();
            if (removed > 0) {
                log.info("cleanup inactive proxy clients. removed:{}", removed);
            }
        } catch (Throwable t) {
            log.warn("cleanup inactive proxy clients failed", t);
        }
    }
}
