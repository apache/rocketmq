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

public class ProxyClientAdminPeerResponse<T> {
    private static final int MAX_ERROR_MESSAGE_LENGTH = 4096;
    private static final String TRUNCATED_ERROR_MESSAGE_SUFFIX = "...(truncated)";

    private final String proxyId;
    private final boolean success;
    private final T body;
    private final String errorCode;
    private final String errorMessage;

    private ProxyClientAdminPeerResponse(String proxyId, boolean success, T body, String errorCode,
        String errorMessage) {
        this.proxyId = requireProxyId(proxyId);
        this.success = success;
        this.body = body;
        this.errorCode = StringUtils.trimToEmpty(errorCode);
        this.errorMessage = normalizeErrorMessage(errorMessage);
    }

    public static <T> ProxyClientAdminPeerResponse<T> success(String proxyId, T body) {
        if (body == null) {
            throw new IllegalArgumentException("body is required");
        }
        return new ProxyClientAdminPeerResponse<>(proxyId, true, body, "", "");
    }

    public static <T> ProxyClientAdminPeerResponse<T> error(String proxyId, String errorCode, String errorMessage) {
        String normalizedErrorCode = StringUtils.trimToNull(errorCode);
        if (normalizedErrorCode == null) {
            throw new IllegalArgumentException("errorCode is required");
        }
        return new ProxyClientAdminPeerResponse<>(proxyId, false, null, normalizedErrorCode, errorMessage);
    }

    public String getProxyId() {
        return proxyId;
    }

    public boolean isSuccess() {
        return success;
    }

    public T getBody() {
        return body;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    private static String requireProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException("proxyId is required");
        }
        return normalizedProxyId;
    }

    private static String normalizeErrorMessage(String errorMessage) {
        String normalizedErrorMessage = StringUtils.trimToEmpty(errorMessage);
        if (normalizedErrorMessage.length() <= MAX_ERROR_MESSAGE_LENGTH) {
            return normalizedErrorMessage;
        }
        return normalizedErrorMessage.substring(
            0,
            MAX_ERROR_MESSAGE_LENGTH - TRUNCATED_ERROR_MESSAGE_SUFFIX.length()
        ) + TRUNCATED_ERROR_MESSAGE_SUFFIX;
    }
}
