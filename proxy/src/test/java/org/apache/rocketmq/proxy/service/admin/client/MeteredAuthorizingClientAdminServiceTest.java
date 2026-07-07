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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MeteredAuthorizingClientAdminServiceTest {

    @Test
    public void recordsUnauthorizedWhenAuthorizationFailsBeforeDelegating() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        List<Record> records = new ArrayList<>();
        AuthorizationException denied = new AuthorizationException("denied");
        doThrow(denied).when(authorizationService).authorize(
            requestContext.getSubject(),
            ClientAdminOperation.LIST_CLIENTS,
            requestContext.getSourceIp()
        );
        MeteredAuthorizingClientAdminService adminService = new MeteredAuthorizingClientAdminService(
            delegate,
            authorizationService,
            (operation, result, latencyMillis) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> adminService.listClients(requestContext, query))
            .isSameAs(denied);

        verify(delegate, never()).listClients(query);
        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS,
            ClientAdminMetricsResult.UNAUTHORIZED,
            1L
        ));
    }

    @Test
    public void recordsInternalErrorWhenDelegateThrowsError() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        List<Record> records = new ArrayList<>();
        LinkageError error = new LinkageError("linkage failed");
        when(delegate.listClients(query)).thenThrow(error);
        MeteredAuthorizingClientAdminService adminService = new MeteredAuthorizingClientAdminService(
            delegate,
            authorizationService,
            (operation, result, latencyMillis) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> adminService.listClients(requestContext, query))
            .isSameAs(error);

        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS,
            ClientAdminMetricsResult.INTERNAL_ERROR,
            1L
        ));
    }

    @Test
    public void recordsTimeoutWhenDelegateThrowsWrappedRemotingTimeout() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        List<Record> records = new ArrayList<>();
        RuntimeException timeout = new RuntimeException(new RemotingTimeoutException("admin query timed out"));
        when(delegate.listClients(query)).thenThrow(timeout);
        MeteredAuthorizingClientAdminService adminService = new MeteredAuthorizingClientAdminService(
            delegate,
            authorizationService,
            (operation, result, latencyMillis) -> records.add(new Record(operation, result, latencyMillis)),
            clock()
        );

        assertThatThrownBy(() -> adminService.listClients(requestContext, query))
            .isSameAs(timeout);

        assertThat(records).containsExactly(new Record(
            ClientAdminOperation.LIST_CLIENTS,
            ClientAdminMetricsResult.TIMEOUT,
            1L
        ));
    }

    @Test
    public void metricsRecorderErrorDoesNotMaskSuccessfulResult() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), null);
        when(delegate.listClients(query)).thenReturn(page);
        ClientAdminMetricsRecorder failingRecorder = (operation, result, latencyMillis) -> {
            throw new LinkageError("metrics linkage down");
        };
        MeteredAuthorizingClientAdminService adminService = new MeteredAuthorizingClientAdminService(
            delegate,
            authorizationService,
            failingRecorder,
            clock()
        );

        assertThat(adminService.listClients(requestContext, query)).isSameAs(page);
    }

    @Test
    public void metricsRecorderErrorDoesNotMaskAuthorizationFailure() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        AuthorizationException denied = new AuthorizationException("denied");
        doThrow(denied).when(authorizationService).authorize(
            requestContext.getSubject(),
            ClientAdminOperation.LIST_CLIENTS,
            requestContext.getSourceIp()
        );
        ClientAdminMetricsRecorder failingRecorder = (operation, result, latencyMillis) -> {
            throw new LinkageError("metrics linkage down");
        };
        MeteredAuthorizingClientAdminService adminService = new MeteredAuthorizingClientAdminService(
            delegate,
            authorizationService,
            failingRecorder,
            clock()
        );

        assertThatThrownBy(() -> adminService.listClients(requestContext, query))
            .isSameAs(denied);
        verify(delegate, never()).listClients(query);
    }

    private static ClientAdminRequestContext requestContext() {
        return ClientAdminRequestContext.of(User.of("admin"), "127.0.0.1");
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
