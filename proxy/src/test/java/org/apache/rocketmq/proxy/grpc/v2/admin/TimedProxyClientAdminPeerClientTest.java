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
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
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
                .isInstanceOf(GrpcProxyException.class)
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
    public void listProxyIdsRestoresInterruptWhenDelegateFailureWrapsInterruptedException() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new WrappedInterruptedListPeerClient(),
                executor,
                1000L
            );

            assertThatThrownBy(client::listProxyIds)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wrapped peer discovery interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            executor.shutdownNow();
        }
    }

    @Test
    public void listProxyIdsCancelsDelegateWhenInterruptedWhileWaiting() {
        InterruptingExecutorService executor = new InterruptingExecutorService();
        TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
            new StaticPeerClient(ProxyClientAdminPeerResponse.success("proxy-a", "ok")),
            executor,
            1000L
        );
        try {
            assertThatThrownBy(client::listProxyIds)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Interrupted")
                .hasMessageContaining("peer discovery");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(executor.wasCancelledWithInterrupt()).isTrue();
        } finally {
            Thread.interrupted();
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
    public void listProxyIdsRejectsEmptyDelegateResult() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new EmptyListPeerClient(),
                executor,
                1000L
            );

            assertThatThrownBy(client::listProxyIds)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at least one peer proxyId");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void listProxyIdsRejectsOverlongDelegateProxyIds() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new OverlongListPeerClient(),
                executor,
                1000L
            );

            assertThatThrownBy(client::listProxyIds)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("peer proxyId length exceeds 255");
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
    public void executeCancelsDelegateWhenInterruptedWhileWaiting() {
        InterruptingExecutorService executor = new InterruptingExecutorService();
        TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
            new StaticPeerClient(ProxyClientAdminPeerResponse.success("proxy-a", "ok")),
            executor,
            1000L
        );
        try {
            ProxyClientAdminPeerResponse<?> response = client.execute(
                ProxyContext.create(),
                "proxy-a",
                request()
            );

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("Interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(executor.wasCancelledWithInterrupt()).isTrue();
        } finally {
            Thread.interrupted();
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
    public void executeRestoresInterruptWhenDelegateFailureWrapsInterruptedException() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TimedProxyClientAdminPeerClient client = new TimedProxyClientAdminPeerClient(
                new WrappedInterruptedExecutePeerClient(),
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
            assertThat(response.getErrorMessage()).contains("wrapped peer execute interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
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

    private static class InterruptingExecutorService extends AbstractExecutorService {
        private InterruptingFuture<?> future;

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            InterruptingFuture<T> interruptingFuture = new InterruptingFuture<>();
            this.future = interruptingFuture;
            return interruptingFuture;
        }

        boolean wasCancelledWithInterrupt() {
            return this.future != null && this.future.wasCancelledWithInterrupt();
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
        }
    }

    private static class InterruptingFuture<T> implements Future<T> {
        private boolean cancelledWithInterrupt;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            this.cancelledWithInterrupt = mayInterruptIfRunning;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelledWithInterrupt;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            throw new InterruptedException("interrupted");
        }

        @Override
        public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            throw new InterruptedException("interrupted");
        }

        boolean wasCancelledWithInterrupt() {
            return cancelledWithInterrupt;
        }
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

    private static class WrappedInterruptedListPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            throw new CompletionException(new InterruptedException("wrapped peer discovery interrupted"));
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

    private static class EmptyListPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            return Collections.emptyList();
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            return ProxyClientAdminPeerResponse.success(proxyId, "ok");
        }
    }

    private static class OverlongListPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList(StringUtils.repeat("p", 256));
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

    private static class WrappedInterruptedExecutePeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList("proxy-a");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            throw new CompletionException(new InterruptedException("wrapped peer execute interrupted"));
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
