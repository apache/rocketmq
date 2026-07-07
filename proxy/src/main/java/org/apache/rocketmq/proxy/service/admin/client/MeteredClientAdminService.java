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
package org.apache.rocketmq.proxy.service.admin.client;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class MeteredClientAdminService implements ClientAdminService {
    private static final ClientAdminMetricsRecorder NOOP_METRICS_RECORDER = (operation, result, latencyMillis,
        scope) -> {
    };
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ClientAdminService delegate;
    private final ClientAdminMetricsRecorder metricsRecorder;
    private final LongSupplier nanoTimeSupplier;

    public MeteredClientAdminService(ClientAdminService delegate, ClientAdminMetricsRecorder metricsRecorder) {
        this(delegate, metricsRecorder, System::nanoTime);
    }

    MeteredClientAdminService(ClientAdminService delegate, ClientAdminMetricsRecorder metricsRecorder,
        LongSupplier nanoTimeSupplier) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate is required");
        }
        this.delegate = delegate;
        this.metricsRecorder = metricsRecorder == null ? NOOP_METRICS_RECORDER : metricsRecorder;
        this.nanoTimeSupplier = nanoTimeSupplier == null ? System::nanoTime : nanoTimeSupplier;
    }

    @Override
    public ProxyClientPage listClients(ProxyClientQuery query) {
        return this.record(
            ClientAdminOperation.LIST_CLIENTS,
            this.scopeOf(query),
            () -> this.delegate.listClients(query)
        );
    }

    @Override
    public ProxyClientInfo describeClient(String clientId) {
        return this.record(
            ClientAdminOperation.DESCRIBE_CLIENT,
            ProxyClientScope.LOCAL_PROXY,
            () -> this.delegate.describeClient(clientId)
        );
    }

    @Override
    public ProxyClientPage listClientsByGroup(String group, ProxyClientQuery query) {
        return this.record(
            ClientAdminOperation.LIST_CLIENTS_BY_GROUP,
            this.scopeOf(query),
            () -> this.delegate.listClientsByGroup(group, query)
        );
    }

    @Override
    public ProxyClientPage listClientsByTopic(String topic, ProxyClientQuery query) {
        return this.record(
            ClientAdminOperation.LIST_CLIENTS_BY_TOPIC,
            this.scopeOf(query),
            () -> this.delegate.listClientsByTopic(topic, query)
        );
    }

    private <T> T record(ClientAdminOperation operation, ProxyClientScope scope, Supplier<T> supplier) {
        long startNanos = this.nanoTimeSupplier.getAsLong();
        ClientAdminMetricsResult result = ClientAdminMetricsResult.OK;
        try {
            return supplier.get();
        } catch (RuntimeException e) {
            result = ClientAdminMetricsClassifier.classify(e);
            throw e;
        } catch (Error e) {
            result = ClientAdminMetricsResult.INTERNAL_ERROR;
            throw e;
        } finally {
            this.recordMetrics(operation, result, this.elapsedMillis(startNanos), scope);
        }
    }

    private void recordMetrics(ClientAdminOperation operation, ClientAdminMetricsResult result, long latencyMillis,
        ProxyClientScope scope) {
        try {
            this.metricsRecorder.record(operation, result, latencyMillis, scope);
        } catch (Throwable e) {
            log.warn("record client admin metrics failed. operation:{}, result:{}, scope:{}",
                operation, result, scope, e);
        }
    }

    private ProxyClientScope scopeOf(ProxyClientQuery query) {
        return query == null ? ProxyClientScope.LOCAL_PROXY : query.getScope();
    }

    private long elapsedMillis(long startNanos) {
        long elapsedNanos = this.nanoTimeSupplier.getAsLong() - startNanos;
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
    }
}
