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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MeteredClientAdminServiceTest {

    @Test
    public void listClientsRecordsSuccessMetrics() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), "");
        when(delegate.listClients(query)).thenReturn(page);
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThat(service.listClients(query)).isSameAs(page);
        assertThat(records).containsExactly(new Record(ClientAdminOperation.LIST_CLIENTS, ClientAdminMetricsResult.OK, 1L));
    }

    @Test
    public void listClientsRecordsQueryScopeMetrics() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), "");
        when(delegate.listClients(query)).thenReturn(page);
        List<ProxyClientScope> scopes = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> scopes.add(scope),
            clock()
        );

        assertThat(service.listClients(query)).isSameAs(page);
        assertThat(scopes).containsExactly(ProxyClientScope.ALL_PROXIES);
    }

    @Test
    public void describeClientRecordsNotFoundAndRethrows() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        when(delegate.describeClient("missing-client")).thenThrow(new NoSuchElementException("missing"));
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> service.describeClient("missing-client"))
            .isInstanceOf(NoSuchElementException.class);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.DESCRIBE_CLIENT,
            ClientAdminMetricsResult.NOT_FOUND,
            1L
        ));
    }

    @Test
    public void describeClientRecordsNotFoundWhenDelegateThrowsWrappedNoSuchElement() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        CompletionException wrapped = new CompletionException(new NoSuchElementException("missing"));
        when(delegate.describeClient("missing-client")).thenThrow(wrapped);
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> service.describeClient("missing-client"))
            .isSameAs(wrapped);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.DESCRIBE_CLIENT,
            ClientAdminMetricsResult.NOT_FOUND,
            1L
        ));
    }

    @Test
    public void listClientsByGroupRecordsBadRequestAndRethrows() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        when(delegate.listClientsByGroup("", query)).thenThrow(new IllegalArgumentException("group is required"));
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> service.listClientsByGroup("", query))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS_BY_GROUP,
            ClientAdminMetricsResult.BAD_REQUEST,
            1L
        ));
    }

    @Test
    public void listClientsByTopicRecordsInternalErrorAndRethrows() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        when(delegate.listClientsByTopic("topic-a", query)).thenThrow(new IllegalStateException("boom"));
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> service.listClientsByTopic("topic-a", query))
            .isInstanceOf(IllegalStateException.class);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS_BY_TOPIC,
            ClientAdminMetricsResult.INTERNAL_ERROR,
            1L
        ));
    }

    @Test
    public void listClientsRecordsUnauthorizedAndRethrows() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        when(delegate.listClients(query)).thenThrow(new AuthorizationException("denied"));
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> service.listClients(query))
            .isInstanceOf(AuthorizationException.class);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS,
            ClientAdminMetricsResult.UNAUTHORIZED,
            1L
        ));
    }

    @Test
    public void listClientsRecordsTimeoutAndRethrowsWrappedRemotingTimeout() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        RuntimeException timeout = new RuntimeException(new RemotingTimeoutException("admin query timed out"));
        when(delegate.listClients(query)).thenThrow(timeout);
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> service.listClients(query))
            .isSameAs(timeout);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS,
            ClientAdminMetricsResult.TIMEOUT,
            1L
        ));
    }

    @Test
    public void listClientsRecordsInternalErrorAndRethrowsError() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        LinkageError error = new LinkageError("linkage failed");
        when(delegate.listClients(query)).thenThrow(error);
        List<Record> records = new ArrayList<>();

        MeteredClientAdminService service = new MeteredClientAdminService(
            delegate,
            (operation, result, latencyMillis, scope) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> service.listClients(query))
            .isSameAs(error);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS,
            ClientAdminMetricsResult.INTERNAL_ERROR,
            1L
        ));
    }

    @Test
    public void metricsRecorderFailureDoesNotMaskSuccessfulResult() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), "");
        when(delegate.listClients(query)).thenReturn(page);
        ClientAdminMetricsRecorder failingRecorder = (operation, result, latencyMillis, scope) -> {
            throw new IllegalStateException("metrics down");
        };
        MeteredClientAdminService service = new MeteredClientAdminService(delegate, failingRecorder, clock());

        assertThat(service.listClients(query)).isSameAs(page);
    }

    @Test
    public void metricsRecorderErrorDoesNotMaskSuccessfulResult() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), "");
        when(delegate.listClients(query)).thenReturn(page);
        ClientAdminMetricsRecorder failingRecorder = (operation, result, latencyMillis, scope) -> {
            throw new LinkageError("metrics linkage down");
        };
        MeteredClientAdminService service = new MeteredClientAdminService(delegate, failingRecorder, clock());

        assertThat(service.listClients(query)).isSameAs(page);
    }

    @Test
    public void metricsRecorderFailureDoesNotMaskDelegateException() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        when(delegate.describeClient("missing-client")).thenThrow(new NoSuchElementException("missing"));
        ClientAdminMetricsRecorder failingRecorder = (operation, result, latencyMillis, scope) -> {
            throw new IllegalStateException("metrics down");
        };
        MeteredClientAdminService service = new MeteredClientAdminService(delegate, failingRecorder, clock());

        assertThatThrownBy(() -> service.describeClient("missing-client"))
            .isInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("missing");
    }

    @Test
    public void metricsRecorderErrorDoesNotMaskDelegateException() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        when(delegate.describeClient("missing-client")).thenThrow(new NoSuchElementException("missing"));
        ClientAdminMetricsRecorder failingRecorder = (operation, result, latencyMillis, scope) -> {
            throw new LinkageError("metrics linkage down");
        };
        MeteredClientAdminService service = new MeteredClientAdminService(delegate, failingRecorder, clock());

        assertThatThrownBy(() -> service.describeClient("missing-client"))
            .isInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("missing");
    }

    private static java.util.function.LongSupplier clock() {
        AtomicLong clock = new AtomicLong(0L);
        return () -> clock.getAndAdd(1_000_000L);
    }

    private static class Record {
        private final ClientAdminOperation operation;
        private final ClientAdminMetricsResult result;
        private final long latencyMillis;

        private Record(ClientAdminOperation operation, ClientAdminMetricsResult result, long latencyMillis) {
            this.operation = operation;
            this.result = result;
            this.latencyMillis = latencyMillis;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Record)) {
                return false;
            }
            Record that = (Record) object;
            return this.operation == that.operation
                && this.result == that.result
                && this.latencyMillis == that.latencyMillis;
        }

        @Override
        public int hashCode() {
            int result = this.operation.hashCode();
            result = 31 * result + this.result.hashCode();
            result = 31 * result + Long.hashCode(this.latencyMillis);
            return result;
        }
    }
}
