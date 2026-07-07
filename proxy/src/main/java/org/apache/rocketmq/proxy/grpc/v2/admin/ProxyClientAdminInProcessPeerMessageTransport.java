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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public class ProxyClientAdminInProcessPeerMessageTransport implements ProxyClientAdminPeerMessageTransport {
    private final Map<String, ProxyClientAdminPeerMessageHandler> handlers;
    private final List<String> proxyIds;
    private final ProxyClientAdminPeerMessageCodec codec;

    public ProxyClientAdminInProcessPeerMessageTransport(
        Map<String, ProxyClientAdminPeerMessageHandler> handlers) {
        this(handlers, ProxyClientAdminPeerMessageCodec.getInstance());
    }

    ProxyClientAdminInProcessPeerMessageTransport(Map<String, ProxyClientAdminPeerMessageHandler> handlers,
        ProxyClientAdminPeerMessageCodec codec) {
        if (handlers == null) {
            throw new IllegalArgumentException("handlers is required");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec is required");
        }
        TreeMap<String, ProxyClientAdminPeerMessageHandler> sortedHandlers = new TreeMap<>();
        for (Map.Entry<String, ProxyClientAdminPeerMessageHandler> entry : handlers.entrySet()) {
            String proxyId = requireProxyId(entry.getKey());
            if (sortedHandlers.containsKey(proxyId)) {
                throw new IllegalArgumentException("Duplicate proxyId: " + proxyId);
            }
            ProxyClientAdminPeerMessageHandler handler = entry.getValue();
            if (handler == null) {
                throw new IllegalArgumentException("handler is required");
            }
            String handlerProxyId = handler.getLocalProxyId();
            if (!proxyId.equals(handlerProxyId)) {
                throw new IllegalArgumentException(
                    "handler proxyId mismatch: expected " + proxyId + ", actual " + handlerProxyId
                );
            }
            sortedHandlers.put(proxyId, handler);
        }
        if (sortedHandlers.isEmpty()) {
            throw new IllegalArgumentException("at least one handler is required");
        }
        this.handlers = Collections.unmodifiableMap(new LinkedHashMap<>(sortedHandlers));
        this.proxyIds = Collections.unmodifiableList(new ArrayList<>(sortedHandlers.keySet()));
        this.codec = codec;
    }

    @Override
    public List<String> listProxyIds() {
        return this.proxyIds;
    }

    @Override
    public String execute(ProxyContext ctx, String proxyId, String requestMessage) {
        String requiredProxyId = requireProxyId(proxyId);
        ProxyClientAdminPeerMessageHandler handler = this.handlers.get(requiredProxyId);
        if (handler == null) {
            return this.encodeError(requiredProxyId, Code.NOT_FOUND, "Proxy not found: " + requiredProxyId);
        }
        String requiredRequestMessage;
        try {
            requiredRequestMessage = this.codec.requireRequestMessage(requestMessage);
        } catch (IllegalArgumentException e) {
            return this.encodeError(requiredProxyId, Code.BAD_REQUEST, e.getMessage());
        }
        try {
            String responseMessage = StringUtils.trimToNull(handler.execute(ctx, requiredRequestMessage));
            if (responseMessage == null) {
                return this.encodeError(requiredProxyId, Code.INTERNAL_SERVER_ERROR,
                    "peer response message is required");
            }
            return responseMessage;
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            return this.encodeError(
                requiredProxyId,
                Code.INTERNAL_SERVER_ERROR,
                StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
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

    private void restoreInterruptedStatus(Throwable t) {
        ProxyClientAdminInterrupts.restoreInterruptedStatus(t);
    }

    private static String requireProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException("proxyId is required");
        }
        return normalizedProxyId;
    }
}
