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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public final class ProxyClientAdminScopeMapper {
    private static final String UNSPECIFIED_PUBLIC_SCOPE = "PROXY_SCOPE_UNSPECIFIED";
    private static final String LOCAL_PROXY_PUBLIC_SCOPE = "PROXY_SCOPE_LOCAL_PROXY";
    private static final String ALL_PROXIES_PUBLIC_SCOPE = "PROXY_SCOPE_ALL_PROXIES";
    private static final String PROXY_ID_PUBLIC_SCOPE = "PROXY_SCOPE_PROXY_ID";
    private static final ProxyClientAdminScopeMapper INSTANCE = new ProxyClientAdminScopeMapper();

    private ProxyClientAdminScopeMapper() {
    }

    public static ProxyClientAdminScopeMapper getInstance() {
        return INSTANCE;
    }

    public ProxyClientScope decode(String publicScopeName) {
        String scopeName = StringUtils.trimToEmpty(publicScopeName);
        if (StringUtils.isBlank(scopeName) || UNSPECIFIED_PUBLIC_SCOPE.equals(scopeName)) {
            return ProxyClientScope.LOCAL_PROXY;
        }
        if (LOCAL_PROXY_PUBLIC_SCOPE.equals(scopeName)) {
            return ProxyClientScope.LOCAL_PROXY;
        }
        if (ALL_PROXIES_PUBLIC_SCOPE.equals(scopeName)) {
            return ProxyClientScope.ALL_PROXIES;
        }
        if (PROXY_ID_PUBLIC_SCOPE.equals(scopeName)) {
            return ProxyClientScope.PROXY_ID;
        }
        try {
            return ProxyClientScope.valueOf(scopeName);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported proxy scope: " + publicScopeName, e);
        }
    }
}
