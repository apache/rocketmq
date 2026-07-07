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
import com.google.protobuf.StringValue;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public class ProxyClientAdminPeerGrpcTransport implements ProxyClientAdminPeerMessageTransport {
    private final Map<String, Channel> channels;
    private final List<String> proxyIds;
    private final Invoker invoker;
    private final ProxyClientAdminPeerMessageCodec codec;

    public ProxyClientAdminPeerGrpcTransport(Map<String, Channel> channels) {
        this(channels, new BlockingUnaryInvoker());
    }

    ProxyClientAdminPeerGrpcTransport(Map<String, Channel> channels, Invoker invoker) {
        this(channels, invoker, ProxyClientAdminPeerMessageCodec.getInstance());
    }

    ProxyClientAdminPeerGrpcTransport(Map<String, Channel> channels, Invoker invoker,
        ProxyClientAdminPeerMessageCodec codec) {
        if (channels == null) {
            throw new IllegalArgumentException("channels is required");
        }
        if (invoker == null) {
            throw new IllegalArgumentException("invoker is required");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec is required");
        }
        TreeMap<String, Channel> sortedChannels = new TreeMap<>();
        for (Map.Entry<String, Channel> entry : channels.entrySet()) {
            String proxyId = requireProxyId(entry.getKey());
            if (sortedChannels.containsKey(proxyId)) {
                throw new IllegalArgumentException("Duplicate proxyId: " + proxyId);
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("channel is required");
            }
            sortedChannels.put(proxyId, entry.getValue());
        }
        if (sortedChannels.isEmpty()) {
            throw new IllegalArgumentException("at least one channel is required");
        }
        this.channels = Collections.unmodifiableMap(new LinkedHashMap<>(sortedChannels));
        this.proxyIds = Collections.unmodifiableList(new ArrayList<>(sortedChannels.keySet()));
        this.invoker = invoker;
        this.codec = codec;
    }

    @Override
    public List<String> listProxyIds() {
        return this.proxyIds;
    }

    @Override
    public String execute(ProxyContext ctx, String proxyId, String requestMessage) {
        String requiredProxyId = requireProxyId(proxyId);
        Channel channel = this.channels.get(requiredProxyId);
        if (channel == null) {
            return this.encodeError(requiredProxyId, Code.NOT_FOUND, "Proxy not found: " + requiredProxyId);
        }
        String requiredRequestMessage;
        ProxyClientAdminPeerRequest peerRequest;
        try {
            requiredRequestMessage = this.codec.requireRequestMessage(requestMessage);
            peerRequest = this.codec.decodeRequest(requiredRequestMessage);
        } catch (IllegalArgumentException e) {
            return this.encodeError(requiredProxyId, Code.BAD_REQUEST, e.getMessage());
        }
        try {
            return this.codec.requireResponseMessage(
                peerRequest.getOperation(),
                this.invoker.execute(channel, requiredRequestMessage, this.buildMetadata(ctx))
            );
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            return this.encodeError(
                requiredProxyId,
                this.statusCode(t),
                this.statusMessage(t)
            );
        }
    }

    private String encodeError(String proxyId, Code code, String message) {
        ProxyClientAdminPeerResponse<ProxyClientPage> response = ProxyClientAdminPeerResponse.error(
            proxyId,
            code.name(),
            message
        );
        return this.codec.encodePageResponse(response);
    }

    private Code statusCode(Throwable t) {
        Status status = this.grpcStatus(t);
        if (status == null) {
            return Code.INTERNAL_SERVER_ERROR;
        }
        switch (status.getCode()) {
            case INVALID_ARGUMENT:
            case FAILED_PRECONDITION:
            case OUT_OF_RANGE:
                return Code.BAD_REQUEST;
            case NOT_FOUND:
                return Code.NOT_FOUND;
            case UNAUTHENTICATED:
            case PERMISSION_DENIED:
                return Code.UNAUTHORIZED;
            case RESOURCE_EXHAUSTED:
                return Code.TOO_MANY_REQUESTS;
            case UNIMPLEMENTED:
                return Code.NOT_IMPLEMENTED;
            case UNAVAILABLE:
            case DEADLINE_EXCEEDED:
                return Code.PROXY_TIMEOUT;
            default:
                return Code.INTERNAL_SERVER_ERROR;
        }
    }

    private String statusMessage(Throwable t) {
        Status status = this.grpcStatus(t);
        if (status != null) {
            String description = StringUtils.trimToNull(status.getDescription());
            if (description != null) {
                return description;
            }
        }
        return StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName());
    }

    private Status grpcStatus(Throwable t) {
        Throwable realException = ExceptionUtils.getRealException(t);
        if (realException instanceof StatusRuntimeException) {
            return ((StatusRuntimeException) realException).getStatus();
        }
        if (realException instanceof StatusException) {
            return ((StatusException) realException).getStatus();
        }
        return null;
    }

    private void restoreInterruptedStatus(Throwable t) {
        ProxyClientAdminInterrupts.restoreInterruptedStatus(t);
    }

    private Metadata buildMetadata(ProxyContext ctx) {
        Metadata metadata = new Metadata();
        Metadata currentMetadata = GrpcConstants.METADATA.get(Context.current());
        if (currentMetadata != null) {
            metadata.merge(currentMetadata);
        }
        if (ctx == null) {
            return metadata;
        }
        this.putIfNotBlank(metadata, GrpcConstants.AUTHORIZATION_AK, this.subjectUsername(ctx.getSubject()));
        this.putIfNotBlank(metadata, GrpcConstants.REMOTE_ADDRESS, ctx.getRemoteAddress());
        this.putIfNotBlank(metadata, GrpcConstants.LOCAL_ADDRESS, ctx.getLocalAddress());
        this.putIfNotBlank(metadata, GrpcConstants.CLIENT_ID, ctx.getClientID());
        this.putIfNotBlank(metadata, GrpcConstants.LANGUAGE, ctx.getLanguage());
        this.putIfNotBlank(metadata, GrpcConstants.CLIENT_VERSION, ctx.getClientVersion());
        this.putIfNotBlank(metadata, GrpcConstants.NAMESPACE_ID, ctx.getNamespace());
        return metadata;
    }

    private void putIfNotBlank(Metadata metadata, Metadata.Key<String> key, String value) {
        String normalizedValue = StringUtils.trimToNull(value);
        if (normalizedValue == null) {
            return;
        }
        metadata.removeAll(key);
        metadata.put(key, normalizedValue);
    }

    private String subjectUsername(Subject subject) {
        if (subject == null) {
            return null;
        }
        if (subject instanceof User) {
            return ((User) subject).getUsername();
        }
        String subjectKey = StringUtils.trimToNull(subject.getSubjectKey());
        String userPrefix = SubjectType.USER.getName() + CommonConstants.COLON;
        if (StringUtils.startsWith(subjectKey, userPrefix)) {
            return StringUtils.substringAfter(subjectKey, CommonConstants.COLON);
        }
        return null;
    }

    private static String requireProxyId(String proxyId) {
        return ProxyClientAdminPeerIds.requireProxyId(proxyId);
    }

    @FunctionalInterface
    interface Invoker {
        String execute(Channel channel, String requestMessage, Metadata metadata);
    }

    private static class BlockingUnaryInvoker implements Invoker {
        @Override
        public String execute(Channel channel, String requestMessage, Metadata metadata) {
            Channel callChannel = metadata == null || metadata.keys().isEmpty()
                ? channel
                : ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
            StringValue response = ClientCalls.blockingUnaryCall(
                callChannel,
                ProxyClientAdminPeerGrpcService.EXECUTE_METHOD,
                CallOptions.DEFAULT,
                StringValue.of(StringUtils.defaultString(requestMessage))
            );
            return response.getValue();
        }
    }
}
