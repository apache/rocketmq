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
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminRequestContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminActivity {
    private final AuthorizingClientAdminService clientAdminService;

    public ProxyClientAdminActivity(AuthorizingClientAdminService clientAdminService) {
        if (clientAdminService == null) {
            throw new IllegalArgumentException("clientAdminService is required");
        }
        this.clientAdminService = clientAdminService;
    }

    public ProxyClientAdminResult<ProxyClientPage> listClients(ProxyContext ctx, ProxyClientQuery query) {
        return this.execute(() -> this.clientAdminService.listClients(ClientAdminRequestContext.from(ctx), query));
    }

    public ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx, String clientId) {
        return this.describeClient(ctx, clientId, ProxyClientScope.LOCAL_PROXY);
    }

    public ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx, String clientId,
        ProxyClientScope scope) {
        return this.execute(() -> {
            this.validateLocalProxyScope(scope);
            return this.clientAdminService.describeClient(ClientAdminRequestContext.from(ctx), clientId);
        });
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByGroup(ProxyContext ctx, String group,
        ProxyClientQuery query) {
        return this.execute(() -> this.clientAdminService.listClientsByGroup(
            ClientAdminRequestContext.from(ctx),
            group,
            query
        ));
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByTopic(ProxyContext ctx, String topic,
        ProxyClientQuery query) {
        return this.execute(() -> this.clientAdminService.listClientsByTopic(
            ClientAdminRequestContext.from(ctx),
            topic,
            query
        ));
    }

    private <T> ProxyClientAdminResult<T> execute(Supplier<T> supplier) {
        try {
            return new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
                supplier.get()
            );
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private void validateLocalProxyScope(ProxyClientScope scope) {
        ProxyClientScope effectiveScope = scope == null ? ProxyClientScope.LOCAL_PROXY : scope;
        if (effectiveScope != ProxyClientScope.LOCAL_PROXY) {
            throw new IllegalArgumentException("Unsupported proxy scope: " + effectiveScope);
        }
    }
}
