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

import com.google.protobuf.StringValue;
import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;

public class ProxyClientAdminPeerGrpcService implements BindableService {
    public static final String SERVICE_NAME = "apache.rocketmq.proxy.v2.ProxyClientAdminPeerService";
    public static final MethodDescriptor<StringValue, StringValue> EXECUTE_METHOD =
        MethodDescriptor.<StringValue, StringValue>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Execute"))
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .build();

    private final ProxyClientAdminContextFactory contextFactory;
    private final ProxyClientAdminPeerMessageHandler messageHandler;
    private final ProxyClientAdminPeerMessageCodec messageCodec = ProxyClientAdminPeerMessageCodec.getInstance();

    public ProxyClientAdminPeerGrpcService(ProxyClientAdminContextFactory contextFactory,
        ProxyClientAdminPeerMessageHandler messageHandler) {
        if (contextFactory == null) {
            throw new IllegalArgumentException("contextFactory is required");
        }
        if (messageHandler == null) {
            throw new IllegalArgumentException("messageHandler is required");
        }
        this.contextFactory = contextFactory;
        this.messageHandler = messageHandler;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(SERVICE_NAME)
            .addMethod(
                EXECUTE_METHOD,
                ServerCalls.asyncUnaryCall(new ServerCalls.UnaryMethod<StringValue, StringValue>() {
                    @Override
                    public void invoke(StringValue request, StreamObserver<StringValue> responseObserver) {
                        execute(request, responseObserver);
                    }
                })
            )
            .build();
    }

    StringValue execute(Metadata headers, StringValue request) {
        StringValue requiredRequest = this.requireRequest(request);
        String requestMessage = this.messageCodec.requireRequestMessage(requiredRequest.getValue());
        ProxyClientAdminPeerRequest peerRequest = this.messageCodec.decodeRequest(requestMessage);
        ProxyContext ctx = this.requireProxyContext(this.contextFactory.create(
            this.normalizeMetadata(headers),
            requiredRequest
        ));
        String responseMessage = StringUtils.trimToNull(this.messageHandler.execute(ctx, requestMessage));
        if (responseMessage == null) {
            throw new IllegalStateException("peer response message is required");
        }
        this.messageCodec.requireResponseMessage(peerRequest.getOperation(), responseMessage);
        return StringValue.of(responseMessage);
    }

    private void execute(StringValue request, StreamObserver<StringValue> responseObserver) {
        try {
            responseObserver.onNext(this.execute(this.currentMetadata(), request));
            responseObserver.onCompleted();
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            ProxyClientAdminGrpcErrorWriter.write(responseObserver, t);
        }
    }

    private Metadata currentMetadata() {
        return this.normalizeMetadata(GrpcConstants.METADATA.get(Context.current()));
    }

    private Metadata normalizeMetadata(Metadata headers) {
        return headers == null ? new Metadata() : headers;
    }

    private StringValue requireRequest(StringValue request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        return request;
    }

    private ProxyContext requireProxyContext(ProxyContext ctx) {
        if (ctx == null) {
            throw new IllegalStateException("proxyContext is required");
        }
        return ctx;
    }

    private void restoreInterruptedStatus(Throwable t) {
        ProxyClientAdminInterrupts.restoreInterruptedStatus(t);
    }
}
