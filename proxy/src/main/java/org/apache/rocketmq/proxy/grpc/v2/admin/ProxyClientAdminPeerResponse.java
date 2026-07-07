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

public class ProxyClientAdminPeerResponse<T> {
    private static final int MAX_PROXY_ID_LENGTH = 255;
    private static final int MAX_ERROR_CODE_LENGTH = 255;
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
        this.errorCode = normalizeErrorCode(success, errorCode);
        this.errorMessage = normalizeErrorMessage(errorMessage);
    }

    public static <T> ProxyClientAdminPeerResponse<T> success(String proxyId, T body) {
        if (body == null) {
            throw new IllegalArgumentException("body is required");
        }
        return new ProxyClientAdminPeerResponse<>(proxyId, true, body, "", "");
    }

    public static <T> ProxyClientAdminPeerResponse<T> error(String proxyId, String errorCode, String errorMessage) {
        return new ProxyClientAdminPeerResponse<>(proxyId, false, null, errorCode, errorMessage);
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
        if (normalizedProxyId.length() > MAX_PROXY_ID_LENGTH) {
            throw new IllegalArgumentException("proxyId length exceeds " + MAX_PROXY_ID_LENGTH);
        }
        return normalizedProxyId;
    }

    private static String normalizeErrorCode(boolean success, String errorCode) {
        if (success) {
            return "";
        }
        String normalizedErrorCode = StringUtils.trimToNull(errorCode);
        if (normalizedErrorCode == null) {
            throw new IllegalArgumentException("errorCode is required");
        }
        if (normalizedErrorCode.length() > MAX_ERROR_CODE_LENGTH) {
            throw new IllegalArgumentException("errorCode length exceeds " + MAX_ERROR_CODE_LENGTH);
        }
        Code parsedCode = parseErrorCode(normalizedErrorCode);
        if (isNonErrorCode(parsedCode)) {
            throw new IllegalArgumentException("errorCode must not be OK or UNRECOGNIZED");
        }
        return normalizedErrorCode;
    }

    private static Code parseErrorCode(String errorCode) {
        try {
            return Code.valueOf(errorCode);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unsupported errorCode: " + errorCode, e);
        }
    }

    private static boolean isNonErrorCode(Code errorCode) {
        return errorCode == Code.OK || errorCode == Code.UNRECOGNIZED;
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
