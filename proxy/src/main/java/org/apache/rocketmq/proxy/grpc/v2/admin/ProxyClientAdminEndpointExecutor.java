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

import apache.rocketmq.v2.Status;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminEndpointExecutor {
    private final ProxyClientAdminContextFactory contextFactory;
    private final ProxyClientAdminEndpointHandler endpointHandler;

    public ProxyClientAdminEndpointExecutor(ProxyClientAdminContextFactory contextFactory,
        ProxyClientAdminEndpointHandler endpointHandler) {
        if (contextFactory == null) {
            throw new IllegalArgumentException("contextFactory is required");
        }
        if (endpointHandler == null) {
            throw new IllegalArgumentException("endpointHandler is required");
        }
        this.contextFactory = contextFactory;
        this.endpointHandler = endpointHandler;
    }

    public <P extends GeneratedMessageV3, R> void listClients(P protoRequest,
        Function<P, ProxyClientAdminListClientsRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.listClients(
            this.currentMetadata(),
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory
        );
    }

    public <P extends GeneratedMessageV3, R> void listClients(Metadata headers, P protoRequest,
        Function<P, ProxyClientAdminListClientsRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.execute(
            headers,
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory,
            this.endpointHandler::listClients
        );
    }

    public <P extends GeneratedMessageV3, R> void describeClient(P protoRequest,
        Function<P, ProxyClientAdminDescribeClientRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminClientView, R> responseFactory) {
        this.describeClient(
            this.currentMetadata(),
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory
        );
    }

    public <P extends GeneratedMessageV3, R> void describeClient(Metadata headers, P protoRequest,
        Function<P, ProxyClientAdminDescribeClientRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminClientView, R> responseFactory) {
        this.execute(
            headers,
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory,
            this.endpointHandler::describeClient
        );
    }

    public <P extends GeneratedMessageV3, R> void listClientsByGroup(P protoRequest,
        Function<P, ProxyClientAdminListClientsByGroupRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.listClientsByGroup(
            this.currentMetadata(),
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory
        );
    }

    public <P extends GeneratedMessageV3, R> void listClientsByGroup(Metadata headers, P protoRequest,
        Function<P, ProxyClientAdminListClientsByGroupRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.execute(
            headers,
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory,
            this.endpointHandler::listClientsByGroup
        );
    }

    public <P extends GeneratedMessageV3, R> void listClientsByTopic(P protoRequest,
        Function<P, ProxyClientAdminListClientsByTopicRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.listClientsByTopic(
            this.currentMetadata(),
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory
        );
    }

    public <P extends GeneratedMessageV3, R> void listClientsByTopic(Metadata headers, P protoRequest,
        Function<P, ProxyClientAdminListClientsByTopicRequest> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.execute(
            headers,
            protoRequest,
            requestAdapter,
            responseObserver,
            responseFactory,
            this.endpointHandler::listClientsByTopic
        );
    }

    private Metadata currentMetadata() {
        return this.normalizeMetadata(GrpcConstants.METADATA.get(Context.current()));
    }

    private Metadata normalizeMetadata(Metadata headers) {
        return headers == null ? new Metadata() : headers;
    }

    private <P extends GeneratedMessageV3, D, T, R> void execute(Metadata headers, P protoRequest,
        Function<P, D> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, T, R> responseFactory,
        EndpointCall<D, T, R> endpointCall) {
        StreamObserver<R> requiredResponseObserver = this.requireResponseObserver(responseObserver);
        BiFunction<Status, T, R> requiredResponseFactory = null;
        try {
            requiredResponseFactory = this.requireResponseFactory(responseFactory);
            Function<P, D> requiredRequestAdapter = this.requireRequestAdapter(requestAdapter);
            P requiredProtoRequest = this.requireProtoRequest(protoRequest);
            D request = this.requirePublicEndpointRequest(
                this.requireAdaptedRequest(requiredRequestAdapter.apply(requiredProtoRequest))
            );
            ProxyContext ctx = this.requireProxyContext(
                this.contextFactory.create(this.normalizeMetadata(headers), requiredProtoRequest)
            );
            endpointCall.execute(ctx, request, requiredResponseObserver, requiredResponseFactory);
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            if (requiredResponseFactory == null) {
                ProxyClientAdminGrpcErrorWriter.write(requiredResponseObserver, t);
                return;
            }
            this.endpointHandler.handle(requiredResponseObserver, () -> {
                return this.throwUnchecked(t);
            }, requiredResponseFactory);
        }
    }

    private <P, D> Function<P, D> requireRequestAdapter(Function<P, D> requestAdapter) {
        if (requestAdapter == null) {
            throw new IllegalArgumentException("requestAdapter is required");
        }
        return requestAdapter;
    }

    private <P> P requireProtoRequest(P protoRequest) {
        if (protoRequest == null) {
            throw new IllegalArgumentException("protoRequest is required");
        }
        return protoRequest;
    }

    private <D> D requireAdaptedRequest(D request) {
        if (request == null) {
            throw new IllegalArgumentException("requestAdapter result is required");
        }
        return request;
    }

    private <D> D requirePublicEndpointRequest(D request) {
        this.requirePublicEndpointScope(request);
        this.requirePublicEndpointIdentifiers(request);
        this.requirePublicEndpointPageToken(request);
        return request;
    }

    private <D> D requirePublicEndpointScope(D request) {
        ProxyClientScope scope = this.scopeOf(request);
        if (scope != null && scope != ProxyClientScope.LOCAL_PROXY) {
            throw new IllegalArgumentException(
                "public proxy admin endpoint only supports LOCAL_PROXY scope: " + scope
            );
        }
        return request;
    }

    private void requirePublicEndpointIdentifiers(Object request) {
        if (request instanceof ProxyClientAdminDescribeClientRequest
            && StringUtils.isBlank(((ProxyClientAdminDescribeClientRequest) request).getClientId())) {
            throw new IllegalArgumentException("clientId is required");
        }
        if (request instanceof ProxyClientAdminListClientsByGroupRequest
            && StringUtils.isBlank(((ProxyClientAdminListClientsByGroupRequest) request).getGroup())) {
            throw new IllegalArgumentException("group is required");
        }
        if (request instanceof ProxyClientAdminListClientsByTopicRequest
            && StringUtils.isBlank(((ProxyClientAdminListClientsByTopicRequest) request).getTopic())) {
            throw new IllegalArgumentException("topic is required");
        }
    }

    private void requirePublicEndpointPageToken(Object request) {
        if (request instanceof ProxyClientAdminListClientsRequest) {
            ((ProxyClientAdminListClientsRequest) request).toQuery();
        }
    }

    private ProxyClientScope scopeOf(Object request) {
        if (request instanceof ProxyClientAdminListClientsRequest) {
            return ((ProxyClientAdminListClientsRequest) request).getScope();
        }
        if (request instanceof ProxyClientAdminDescribeClientRequest) {
            return ((ProxyClientAdminDescribeClientRequest) request).getScope();
        }
        return null;
    }

    private ProxyContext requireProxyContext(ProxyContext ctx) {
        if (ctx == null) {
            throw new IllegalStateException("proxyContext is required");
        }
        return ctx;
    }

    private <T, R> BiFunction<Status, T, R> requireResponseFactory(BiFunction<Status, T, R> responseFactory) {
        if (responseFactory == null) {
            throw new IllegalArgumentException("responseFactory is required");
        }
        return responseFactory;
    }

    private <R> StreamObserver<R> requireResponseObserver(StreamObserver<R> responseObserver) {
        if (responseObserver == null) {
            throw new IllegalArgumentException("responseObserver is required");
        }
        return responseObserver;
    }

    private void restoreInterruptedStatus(Throwable t) {
        ProxyClientAdminInterrupts.restoreInterruptedStatus(t);
    }

    private <T> T throwUnchecked(Throwable t) {
        ProxyClientAdminEndpointExecutor.<RuntimeException>throwAny(t);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwAny(Throwable t) throws E {
        throw (E) t;
    }

    private interface EndpointCall<D, T, R> {
        void execute(ProxyContext ctx, D request, StreamObserver<R> responseObserver,
            BiFunction<Status, T, R> responseFactory);
    }
}
