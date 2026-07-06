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

public class ProxyClientAdminPeerGrpcTarget {
    private final String proxyId;
    private final String host;
    private final int port;

    public ProxyClientAdminPeerGrpcTarget(String proxyId, String host, int port) {
        this.proxyId = requireProxyId(proxyId);
        this.host = requireHost(host);
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        this.port = port;
    }

    public String getProxyId() {
        return proxyId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    private static String requireProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException("proxyId is required");
        }
        return normalizedProxyId;
    }

    private static String requireHost(String host) {
        String normalizedHost = StringUtils.trimToNull(host);
        if (normalizedHost == null) {
            throw new IllegalArgumentException("host is required");
        }
        return normalizedHost;
    }
}
