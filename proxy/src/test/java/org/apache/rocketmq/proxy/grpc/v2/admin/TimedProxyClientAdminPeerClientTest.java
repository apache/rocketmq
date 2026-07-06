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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TimedProxyClientAdminPeerClientTest {

    @Test
    public void listProxyIdsDelegates() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new StaticPeerClient(ProxyClientAdminPeerResponse.success("proxy-a", "ok")),
                executor,
                1000L
            );

            List<String> proxyIds = client.listProxyIds();

            assertThat(proxyIds).containsExactly("proxy-a");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void listProxyIdsTimesOutAndCancelsDelegateOnTimeout() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new BlockingListPeerClient(entered, interrupted),
                executor,
                10L
            );

            assertThatThrownBy(client::listProxyIds)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Timed out")
                .hasMessageContaining("peer discovery");
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void listProxyIdsThrowsInternalErrorWhenDelegateThrows() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new ThrowingListPeerClient(),
                executor,
                1000L
            );

            assertThatThrownBy(client::listProxyIds)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("boom");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void listProxyIdsRejectsNullDelegateResult() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new NullListPeerClient(),
                executor,
                1000L
            );

            assertThatThrownBy(client::listProxyIds)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("peer proxyIds are required");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void executeReturnsDelegateResponseBeforeTimeout() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            ProxyClientAdminPeerResponse<String> delegateResponse =
                ProxyClientAdminPeerResponse.success("proxy-a", "ok");
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new StaticPeerClient(delegateResponse),
                executor,
                1000L
            );

            ProxyClientAdminPeerResponse<?> response = client.execute(
                ProxyContext.create(),
                "proxy-a",
                request()
            );

            assertThat(response).isSameAs(delegateResponse);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void executeReturnsProxyTimeoutAndCancelsDelegateOnTimeout() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new BlockingPeerClient(entered, interrupted),
                executor,
                10L
            );

            ProxyClientAdminPeerResponse<?> response = client.execute(
                ProxyContext.create(),
                "proxy-a",
                request()
            );

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getErrorCode()).isEqualTo(Code.PROXY_TIMEOUT.name());
            assertThat(response.getErrorMessage()).contains("Timed out");
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void executeReturnsInternalServerErrorWhenDelegateThrows() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new ThrowingPeerClient(),
                executor,
                1000L
            );

            ProxyClientAdminPeerResponse<?> response = client.execute(
                ProxyContext.create(),
                "proxy-a",
                request()
            );

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("boom");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void executeMapsNullDelegateResponseToInternalServerError() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new NullResponsePeerClient(),
                executor,
                1000L
            );

            ProxyClientAdminPeerResponse<?> response = client.execute(
                ProxyContext.create(),
                "proxy-a",
                request()
            );

            assertThat(response).isNotNull();
            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("peer response is required");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void constructorRejectsInvalidArguments() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertThatThrownBy(() -> new TimedProxyClientAdminPeerClient(null, executor, 1000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("delegate is required");
            assertThatThrownBy(() -> new TimedProxyClientAdminPeerClient(
                new StaticPeerClient(ProxyClientAdminPeerResponse.success("proxy-a", "ok")),
                null,
                1000L
            ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("executorService is required");
            assertThatThrownBy(() -> new TimedProxyClientAdminPeerClient(
                new StaticPeerClient(ProxyClientAdminPeerResponse.success("proxy-a", "ok")),
                executor,
                0L
            ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeoutMillis must be positive");
        } finally {
            executor.shutdownNow();
        }
    }

    private static ProxyClientAdminPeerRequest request() {
        return ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .build();
    }

    private static class StaticPeerClient implements ProxyClientAdminPeerClient {
        private final ProxyClientAdminPeerResponse<?> response;

        private StaticPeerClient(ProxyClientAdminPeerResponse<?> response) {
            this.response = response;
        }

        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList("proxy-a");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            return this.response;
        }
    }

    private static class BlockingPeerClient implements ProxyClientAdminPeerClient {
        private final CountDownLatch entered;
        private final CountDownLatch interrupted;

        private BlockingPeerClient(CountDownLatch entered, CountDownLatch interrupted) {
            this.entered = entered;
            this.interrupted = interrupted;
        }

        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList("proxy-a");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            this.entered.countDown();
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException ignored) {
                this.interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return ProxyClientAdminPeerResponse.success(proxyId, "late");
        }
    }

    private static class BlockingListPeerClient implements ProxyClientAdminPeerClient {
        private final CountDownLatch entered;
        private final CountDownLatch interrupted;

        private BlockingListPeerClient(CountDownLatch entered, CountDownLatch interrupted) {
            this.entered = entered;
            this.interrupted = interrupted;
        }

        @Override
        public List<String> listProxyIds() {
            this.entered.countDown();
            try {
                Thread.sleep(500L);
            } catch (InterruptedException ignored) {
                this.interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return Collections.singletonList("proxy-a");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            return ProxyClientAdminPeerResponse.success(proxyId, "ok");
        }
    }

    private static class ThrowingListPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            throw new IllegalStateException("boom");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            return ProxyClientAdminPeerResponse.success(proxyId, "ok");
        }
    }

    private static class NullListPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            return null;
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            return ProxyClientAdminPeerResponse.success(proxyId, "ok");
        }
    }

    private static class ThrowingPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList("proxy-a");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            throw new IllegalStateException("boom");
        }
    }

    private static class NullResponsePeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList("proxy-a");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            return null;
        }
    }
}
