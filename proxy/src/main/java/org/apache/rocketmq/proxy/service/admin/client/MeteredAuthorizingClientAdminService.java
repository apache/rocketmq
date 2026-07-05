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

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class MeteredAuthorizingClientAdminService extends AuthorizingClientAdminService {
    private static final ClientAdminMetricsRecorder NOOP_METRICS_RECORDER = (operation, result, latencyMillis) -> {
    };
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ClientAdminMetricsRecorder metricsRecorder;
    private final LongSupplier nanoTimeSupplier;

    public MeteredAuthorizingClientAdminService(ClientAdminService clientAdminService,
        ClientAdminAuthorizationService authorizationService, ClientAdminMetricsRecorder metricsRecorder) {
        this(clientAdminService, authorizationService, metricsRecorder, System::nanoTime);
    }

    MeteredAuthorizingClientAdminService(ClientAdminService clientAdminService,
        ClientAdminAuthorizationService authorizationService, ClientAdminMetricsRecorder metricsRecorder,
        LongSupplier nanoTimeSupplier) {
        super(clientAdminService, authorizationService);
        this.metricsRecorder = metricsRecorder == null ? NOOP_METRICS_RECORDER : metricsRecorder;
        this.nanoTimeSupplier = nanoTimeSupplier == null ? System::nanoTime : nanoTimeSupplier;
    }

    @Override
    public ProxyClientPage listClients(ClientAdminRequestContext requestContext, ProxyClientQuery query) {
        return this.record(
            ClientAdminOperation.LIST_CLIENTS,
            () -> super.listClients(requestContext, query)
        );
    }

    @Override
    public ProxyClientInfo describeClient(ClientAdminRequestContext requestContext, String clientId) {
        return this.record(
            ClientAdminOperation.DESCRIBE_CLIENT,
            () -> super.describeClient(requestContext, clientId)
        );
    }

    @Override
    public ProxyClientPage listClientsByGroup(ClientAdminRequestContext requestContext, String group,
        ProxyClientQuery query) {
        return this.record(
            ClientAdminOperation.LIST_CLIENTS_BY_GROUP,
            () -> super.listClientsByGroup(requestContext, group, query)
        );
    }

    @Override
    public ProxyClientPage listClientsByTopic(ClientAdminRequestContext requestContext, String topic,
        ProxyClientQuery query) {
        return this.record(
            ClientAdminOperation.LIST_CLIENTS_BY_TOPIC,
            () -> super.listClientsByTopic(requestContext, topic, query)
        );
    }

    private <T> T record(ClientAdminOperation operation, Supplier<T> supplier) {
        long startNanos = this.nanoTimeSupplier.getAsLong();
        ClientAdminMetricsResult result = ClientAdminMetricsResult.OK;
        try {
            return supplier.get();
        } catch (RuntimeException e) {
            result = this.classify(e);
            throw e;
        } finally {
            this.recordMetrics(operation, result, this.elapsedMillis(startNanos));
        }
    }

    private void recordMetrics(ClientAdminOperation operation, ClientAdminMetricsResult result, long latencyMillis) {
        try {
            this.metricsRecorder.record(operation, result, latencyMillis);
        } catch (RuntimeException e) {
            log.warn("record client admin metrics failed. operation:{}, result:{}", operation, result, e);
        }
    }

    private ClientAdminMetricsResult classify(RuntimeException exception) {
        if (exception instanceof IllegalArgumentException) {
            return ClientAdminMetricsResult.BAD_REQUEST;
        }
        if (exception instanceof NoSuchElementException) {
            return ClientAdminMetricsResult.NOT_FOUND;
        }
        if (exception instanceof AuthenticationException || exception instanceof AuthorizationException) {
            return ClientAdminMetricsResult.UNAUTHORIZED;
        }
        return ClientAdminMetricsResult.INTERNAL_ERROR;
    }

    private long elapsedMillis(long startNanos) {
        long elapsedNanos = this.nanoTimeSupplier.getAsLong() - startNanos;
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
    }
}
