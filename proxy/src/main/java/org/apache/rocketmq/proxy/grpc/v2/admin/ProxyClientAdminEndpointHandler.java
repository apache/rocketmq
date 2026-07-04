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
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;

public class ProxyClientAdminEndpointHandler {
    private final ProxyClientAdminActivity proxyClientAdminActivity;

    public ProxyClientAdminEndpointHandler() {
        this(null);
    }

    public ProxyClientAdminEndpointHandler(ProxyClientAdminActivity proxyClientAdminActivity) {
        this.proxyClientAdminActivity = proxyClientAdminActivity;
    }

    public <R> void listClients(ProxyContext ctx, ProxyClientAdminListClientsRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.requireProxyClientAdminActivity().listClientViews(ctx, request),
            responseFactory
        );
    }

    public <R> void describeClient(ProxyContext ctx, ProxyClientAdminDescribeClientRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminClientView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.requireProxyClientAdminActivity().describeClientView(ctx, request),
            responseFactory
        );
    }

    public <R> void listClientsByGroup(ProxyContext ctx, ProxyClientAdminListClientsByGroupRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.requireProxyClientAdminActivity().listClientViewsByGroup(ctx, request),
            responseFactory
        );
    }

    public <R> void listClientsByTopic(ProxyContext ctx, ProxyClientAdminListClientsByTopicRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.requireProxyClientAdminActivity().listClientViewsByTopic(ctx, request),
            responseFactory
        );
    }

    public <T, R> void handle(StreamObserver<R> responseObserver,
        Supplier<ProxyClientAdminResult<T>> action,
        BiFunction<Status, T, R> responseFactory) {
        ProxyClientAdminResult<T> result = this.execute(action);
        R response = this.requireResponseFactory(responseFactory).apply(result.getStatus(), result.getBody());
        ResponseWriter.getInstance().write(responseObserver, response);
    }

    private <T> ProxyClientAdminResult<T> execute(Supplier<ProxyClientAdminResult<T>> action) {
        try {
            ProxyClientAdminResult<T> result = this.requireAction(action).get();
            if (result == null) {
                throw new IllegalArgumentException("result is required");
            }
            return result;
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private <T> Supplier<ProxyClientAdminResult<T>> requireAction(Supplier<ProxyClientAdminResult<T>> action) {
        if (action == null) {
            throw new IllegalArgumentException("action is required");
        }
        return action;
    }

    private <T, R> BiFunction<Status, T, R> requireResponseFactory(BiFunction<Status, T, R> responseFactory) {
        if (responseFactory == null) {
            throw new IllegalArgumentException("responseFactory is required");
        }
        return responseFactory;
    }

    private ProxyClientAdminActivity requireProxyClientAdminActivity() {
        if (this.proxyClientAdminActivity == null) {
            throw new IllegalArgumentException("proxyClientAdminActivity is required");
        }
        return this.proxyClientAdminActivity;
    }
}
