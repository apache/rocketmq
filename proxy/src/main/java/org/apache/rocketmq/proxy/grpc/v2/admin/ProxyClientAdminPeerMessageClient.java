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
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public class ProxyClientAdminPeerMessageClient implements ProxyClientAdminPeerClient {
    private final ProxyClientAdminPeerMessageTransport transport;
    private final ProxyClientAdminPeerMessageCodec codec;

    public ProxyClientAdminPeerMessageClient(ProxyClientAdminPeerMessageTransport transport) {
        this(transport, ProxyClientAdminPeerMessageCodec.getInstance());
    }

    ProxyClientAdminPeerMessageClient(ProxyClientAdminPeerMessageTransport transport,
        ProxyClientAdminPeerMessageCodec codec) {
        if (transport == null) {
            throw new IllegalArgumentException("transport is required");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec is required");
        }
        this.transport = transport;
        this.codec = codec;
    }

    @Override
    public List<String> listProxyIds() {
        return this.transport.listProxyIds();
    }

    @Override
    public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
        ProxyClientAdminPeerRequest request) {
        String requiredProxyId = requireProxyId(proxyId);
        if (request == null) {
            return badRequest(requiredProxyId, "request is required");
        }
        try {
            ProxyClientAdminPeerRequest requiredRequest = request;
            String responseMessage = this.transport.execute(
                ctx,
                requiredProxyId,
                this.codec.encodeRequest(requiredRequest)
            );
            return this.decodeResponse(requiredRequest.getOperation(), responseMessage);
        } catch (Throwable t) {
            return internalServerError(requiredProxyId, t);
        }
    }

    private ProxyClientAdminPeerResponse<?> decodeResponse(ProxyClientAdminPeerOperation operation,
        String responseMessage) {
        if (operation == ProxyClientAdminPeerOperation.DESCRIBE_CLIENT) {
            return this.decodeClientResponse(responseMessage);
        }
        return this.decodePageResponse(responseMessage);
    }

    private ProxyClientAdminPeerResponse<ProxyClientPage> decodePageResponse(String responseMessage) {
        return this.codec.decodePageResponse(responseMessage);
    }

    private ProxyClientAdminPeerResponse<ProxyClientInfo> decodeClientResponse(String responseMessage) {
        return this.codec.decodeClientResponse(responseMessage);
    }

    private static ProxyClientAdminPeerResponse<?> internalServerError(String proxyId, Throwable t) {
        return ProxyClientAdminPeerResponse.error(
            proxyId,
            Code.INTERNAL_SERVER_ERROR.name(),
            StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
        );
    }

    private static ProxyClientAdminPeerResponse<?> badRequest(String proxyId, String message) {
        return ProxyClientAdminPeerResponse.error(proxyId, Code.BAD_REQUEST.name(), message);
    }

    private static String requireProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException("proxyId is required");
        }
        return normalizedProxyId;
    }
}
