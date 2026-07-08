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

final class ProxyClientAdminPeerIds {
    private static final int MAX_PROXY_ID_LENGTH = 255;

    private ProxyClientAdminPeerIds() {
    }

    static String requireProxyId(String proxyId) {
        return requireProxyId(proxyId, "proxyId");
    }

    static String requirePeerProxyId(String proxyId) {
        return requireProxyId(proxyId, "peer proxyId");
    }

    static String requireLocalProxyId(String proxyId) {
        return requireProxyId(proxyId, "localProxyId");
    }

    private static String requireProxyId(String proxyId, String fieldName) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
        if (normalizedProxyId.length() > MAX_PROXY_ID_LENGTH) {
            throw new IllegalArgumentException(fieldName + " length exceeds " + MAX_PROXY_ID_LENGTH);
        }
        return normalizedProxyId;
    }
}
