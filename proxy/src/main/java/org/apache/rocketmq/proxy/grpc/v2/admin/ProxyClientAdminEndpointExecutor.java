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
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.rocketmq.proxy.common.ProxyContext;

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

    private <P extends GeneratedMessageV3, D, T, R> void execute(Metadata headers, P protoRequest,
        Function<P, D> requestAdapter,
        StreamObserver<R> responseObserver,
        BiFunction<Status, T, R> responseFactory,
        EndpointCall<D, T, R> endpointCall) {
        StreamObserver<R> requiredResponseObserver = this.requireResponseObserver(responseObserver);
        BiFunction<Status, T, R> requiredResponseFactory = this.requireResponseFactory(responseFactory);
        Function<P, D> requiredRequestAdapter = this.requireRequestAdapter(requestAdapter);
        try {
            ProxyContext ctx = this.contextFactory.create(headers, protoRequest);
            D request = this.requireAdaptedRequest(requiredRequestAdapter.apply(protoRequest));
            endpointCall.execute(ctx, request, requiredResponseObserver, requiredResponseFactory);
        } catch (RuntimeException | Error t) {
            this.endpointHandler.handle(requiredResponseObserver, () -> {
                throw t;
            }, requiredResponseFactory);
        }
    }

    private <P, D> Function<P, D> requireRequestAdapter(Function<P, D> requestAdapter) {
        if (requestAdapter == null) {
            throw new IllegalArgumentException("requestAdapter is required");
        }
        return requestAdapter;
    }

    private <D> D requireAdaptedRequest(D request) {
        if (request == null) {
            throw new IllegalArgumentException("requestAdapter result is required");
        }
        return request;
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

    private interface EndpointCall<D, T, R> {
        void execute(ProxyContext ctx, D request, StreamObserver<R> responseObserver,
            BiFunction<Status, T, R> responseFactory);
    }
}
