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
import apache.rocketmq.v2.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;

public class ProxyClientAdminEndpointHandler {
    private final ProxyClientAdminActivity proxyClientAdminActivity;
    private final ProxyClientAdminScopeRouter proxyClientAdminScopeRouter;

    public ProxyClientAdminEndpointHandler() {
        this(null, null);
    }

    public ProxyClientAdminEndpointHandler(ProxyClientAdminActivity proxyClientAdminActivity) {
        this(proxyClientAdminActivity, null);
    }

    public ProxyClientAdminEndpointHandler(ProxyClientAdminScopeRouter proxyClientAdminScopeRouter) {
        this(null, proxyClientAdminScopeRouter);
    }

    private ProxyClientAdminEndpointHandler(ProxyClientAdminActivity proxyClientAdminActivity,
        ProxyClientAdminScopeRouter proxyClientAdminScopeRouter) {
        this.proxyClientAdminActivity = proxyClientAdminActivity;
        this.proxyClientAdminScopeRouter = proxyClientAdminScopeRouter;
    }

    public <R> void listClients(ProxyContext ctx, ProxyClientAdminListClientsRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.listClientViews(ctx, request),
            responseFactory
        );
    }

    public <R> void describeClient(ProxyContext ctx, ProxyClientAdminDescribeClientRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminClientView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.describeClientView(ctx, request),
            responseFactory
        );
    }

    public <R> void listClientsByGroup(ProxyContext ctx, ProxyClientAdminListClientsByGroupRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.listClientViewsByGroup(ctx, request),
            responseFactory
        );
    }

    public <R> void listClientsByTopic(ProxyContext ctx, ProxyClientAdminListClientsByTopicRequest request,
        StreamObserver<R> responseObserver,
        BiFunction<Status, ProxyClientAdminPageView, R> responseFactory) {
        this.handle(
            responseObserver,
            () -> this.listClientViewsByTopic(ctx, request),
            responseFactory
        );
    }

    public <T, R> void handle(StreamObserver<R> responseObserver,
        Supplier<ProxyClientAdminResult<T>> action,
        BiFunction<Status, T, R> responseFactory) {
        StreamObserver<R> requiredResponseObserver = this.requireResponseObserver(responseObserver);
        BiFunction<Status, T, R> requiredResponseFactory;
        try {
            requiredResponseFactory = this.requireResponseFactory(responseFactory);
        } catch (Throwable t) {
            ProxyClientAdminGrpcErrorWriter.write(requiredResponseObserver, t);
            return;
        }
        ProxyClientAdminResult<T> result = this.execute(action);
        try {
            R response = this.applyResponseFactory(requiredResponseFactory, result);
            ResponseWriter.getInstance().write(requiredResponseObserver, response);
        } catch (Throwable t) {
            ProxyClientAdminGrpcErrorWriter.write(requiredResponseObserver, t);
        }
    }

    private <T, R> R applyResponseFactory(BiFunction<Status, T, R> responseFactory,
        ProxyClientAdminResult<T> result) {
        try {
            return this.requireResponse(responseFactory.apply(result.getStatus(), result.getBody()));
        } catch (Throwable t) {
            return this.requireResponse(responseFactory.apply(ResponseBuilder.getInstance().buildStatus(t), null));
        }
    }

    private <T> ProxyClientAdminResult<T> execute(Supplier<ProxyClientAdminResult<T>> action) {
        try {
            ProxyClientAdminResult<T> result = this.requireAction(action).get();
            if (result == null) {
                throw new IllegalArgumentException("result is required");
            }
            if (result.getStatus().getCode() != Code.OK) {
                return new ProxyClientAdminResult<>(result.getStatus(), null);
            }
            if (result.getBody() == null) {
                throw new IllegalStateException("result body is required");
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

    private <R> StreamObserver<R> requireResponseObserver(StreamObserver<R> responseObserver) {
        if (responseObserver == null) {
            throw new IllegalArgumentException("responseObserver is required");
        }
        return responseObserver;
    }

    private <R> R requireResponse(R response) {
        if (response == null) {
            throw new IllegalStateException("response is required");
        }
        return response;
    }

    private ProxyClientAdminResult<ProxyClientAdminPageView> listClientViews(ProxyContext ctx,
        ProxyClientAdminListClientsRequest request) {
        if (this.proxyClientAdminScopeRouter != null) {
            return this.proxyClientAdminScopeRouter.listClientViews(ctx, request);
        }
        return this.requireProxyClientAdminActivity().listClientViews(ctx, request);
    }

    private ProxyClientAdminResult<ProxyClientAdminClientView> describeClientView(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        if (this.proxyClientAdminScopeRouter != null) {
            return this.proxyClientAdminScopeRouter.describeClientView(ctx, request);
        }
        return this.requireProxyClientAdminActivity().describeClientView(ctx, request);
    }

    private ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByGroup(ProxyContext ctx,
        ProxyClientAdminListClientsByGroupRequest request) {
        if (this.proxyClientAdminScopeRouter != null) {
            return this.proxyClientAdminScopeRouter.listClientViewsByGroup(ctx, request);
        }
        return this.requireProxyClientAdminActivity().listClientViewsByGroup(ctx, request);
    }

    private ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByTopic(ProxyContext ctx,
        ProxyClientAdminListClientsByTopicRequest request) {
        if (this.proxyClientAdminScopeRouter != null) {
            return this.proxyClientAdminScopeRouter.listClientViewsByTopic(ctx, request);
        }
        return this.requireProxyClientAdminActivity().listClientViewsByTopic(ctx, request);
    }

    private ProxyClientAdminActivity requireProxyClientAdminActivity() {
        if (this.proxyClientAdminActivity == null) {
            throw new IllegalStateException("proxyClientAdminActivity is required");
        }
        return this.proxyClientAdminActivity;
    }
}
