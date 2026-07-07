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

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;

public class TimedProxyClientAdminPeerClient implements ProxyClientAdminPeerClient {
    private final ProxyClientAdminPeerClient delegate;
    private final ExecutorService executorService;
    private final long timeoutMillis;

    public TimedProxyClientAdminPeerClient(ProxyClientAdminPeerClient delegate,
        ExecutorService executorService, long timeoutMillis) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate is required");
        }
        if (executorService == null) {
            throw new IllegalArgumentException("executorService is required");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be positive");
        }
        this.delegate = delegate;
        this.executorService = executorService;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public List<String> listProxyIds() {
        Future<List<String>> future;
        try {
            future = this.executorService.submit(this.delegate::listProxyIds);
        } catch (RejectedExecutionException e) {
            throw peerDiscoveryError(e);
        }
        try {
            return requireProxyIds(future.get(this.timeoutMillis, TimeUnit.MILLISECONDS));
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new GrpcProxyException(
                Code.PROXY_TIMEOUT,
                "Timed out waiting for proxy client admin peer discovery after "
                    + this.timeoutMillis + " ms",
                e
            );
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "Interrupted while waiting for proxy client admin peer discovery",
                e
            );
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            ProxyClientAdminInterrupts.restoreInterruptedStatus(cause);
            throw peerDiscoveryError(cause);
        }
    }

    private static List<String> requireProxyIds(List<String> proxyIds) {
        if (proxyIds == null) {
            throw new IllegalStateException("peer proxyIds are required");
        }
        if (proxyIds.isEmpty()) {
            throw new IllegalStateException("at least one peer proxyId is required");
        }
        List<String> normalizedProxyIds = new ArrayList<>(proxyIds.size());
        for (String proxyId : proxyIds) {
            try {
                normalizedProxyIds.add(ProxyClientAdminPeerIds.requirePeerProxyId(proxyId));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
        return Collections.unmodifiableList(normalizedProxyIds);
    }

    @Override
    public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
        ProxyClientAdminPeerRequest request) {
        String requiredProxyId = requireProxyId(proxyId);
        Future<ProxyClientAdminPeerResponse<?>> future;
        try {
            future = this.executorService.submit(
                () -> this.delegate.execute(ctx, requiredProxyId, request)
            );
        } catch (RejectedExecutionException e) {
            return internalServerError(requiredProxyId, e);
        }
        try {
            return requireResponse(requiredProxyId, future.get(this.timeoutMillis, TimeUnit.MILLISECONDS));
        } catch (TimeoutException e) {
            future.cancel(true);
            return ProxyClientAdminPeerResponse.error(
                requiredProxyId,
                Code.PROXY_TIMEOUT.name(),
                "Timed out waiting for proxy client admin peer " + requiredProxyId
                    + " after " + this.timeoutMillis + " ms"
            );
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            return ProxyClientAdminPeerResponse.error(
                requiredProxyId,
                Code.INTERNAL_SERVER_ERROR.name(),
                "Interrupted while waiting for proxy client admin peer " + requiredProxyId
            );
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            ProxyClientAdminInterrupts.restoreInterruptedStatus(cause);
            return internalServerError(requiredProxyId, cause);
        }
    }

    private static ProxyClientAdminPeerResponse<?> requireResponse(String proxyId,
        ProxyClientAdminPeerResponse<?> response) {
        if (response == null) {
            return internalServerError(proxyId, new IllegalStateException("peer response is required"));
        }
        return response;
    }

    private static ProxyClientAdminPeerResponse<?> internalServerError(String proxyId, Throwable t) {
        return ProxyClientAdminPeerResponse.error(
            proxyId,
            Code.INTERNAL_SERVER_ERROR.name(),
            StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
        );
    }

    private static IllegalStateException peerDiscoveryError(Throwable t) {
        return new IllegalStateException(
            StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName()),
            t
        );
    }

    private static String requireProxyId(String proxyId) {
        return ProxyClientAdminPeerIds.requireProxyId(proxyId);
    }
}
