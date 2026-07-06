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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public class ProxyClientAdminPeerMessageHandler {
    private final ProxyClientAdminPeerLocalExecutor localExecutor;
    private final ProxyClientAdminPeerMessageCodec codec;

    public ProxyClientAdminPeerMessageHandler(ProxyClientAdminPeerLocalExecutor localExecutor) {
        this(localExecutor, ProxyClientAdminPeerMessageCodec.getInstance());
    }

    ProxyClientAdminPeerMessageHandler(ProxyClientAdminPeerLocalExecutor localExecutor,
        ProxyClientAdminPeerMessageCodec codec) {
        if (localExecutor == null) {
            throw new IllegalArgumentException("localExecutor is required");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec is required");
        }
        this.localExecutor = localExecutor;
        this.codec = codec;
    }

    public String execute(ProxyContext ctx, String requestMessage) {
        ProxyClientAdminPeerOperation operation = null;
        try {
            ProxyClientAdminPeerRequest request = this.codec.decodeRequest(requestMessage);
            operation = request.getOperation();
            return this.encodeResponse(operation, this.localExecutor.execute(ctx, request));
        } catch (Throwable t) {
            return this.encodeError(operation, t);
        }
    }

    private String encodeError(ProxyClientAdminPeerOperation operation, Throwable t) {
        return this.encodeResponse(
            operation,
            ProxyClientAdminPeerResponse.error(
                this.localExecutor.getLocalProxyId(),
                Code.INTERNAL_SERVER_ERROR.name(),
                StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
            )
        );
    }

    @SuppressWarnings("unchecked")
    private String encodeResponse(ProxyClientAdminPeerOperation operation, ProxyClientAdminPeerResponse<?> response) {
        if (operation == ProxyClientAdminPeerOperation.DESCRIBE_CLIENT) {
            return this.codec.encodeClientResponse((ProxyClientAdminPeerResponse<ProxyClientInfo>) response);
        }
        return this.codec.encodePageResponse((ProxyClientAdminPeerResponse<ProxyClientPage>) response);
    }
}
