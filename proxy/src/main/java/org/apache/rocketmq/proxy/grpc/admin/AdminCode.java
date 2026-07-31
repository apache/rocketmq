/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.admin;

/**
 * Admin API response codes (RIP-2 §5.2.3).
 * Mirrors the AdminCode enum defined in proxy_admin.proto.
 * <p>
 * These codes provide structured error information in the response body,
 * complementing gRPC status codes with application-level semantics.
 */
public enum AdminCode {
    /**
     * Success.
     */
    OK(1, "OK"),

    /**
     * Internal server error.
     */
    INTERNAL_ERROR(2, "Internal error"),

    /**
     * Bad request - invalid parameters.
     */
    BAD_REQUEST(3, "Bad request"),

    /**
     * Unauthorized - authentication required.
     */
    UNAUTHORIZED(4, "Unauthorized"),

    /**
     * Forbidden - insufficient permissions.
     */
    FORBIDDEN(5, "Forbidden"),

    /**
     * Resource not found.
     */
    NOT_FOUND(6, "Not found"),

    /**
     * Too many requests - rate limited.
     */
    TOO_MANY_REQUESTS(7, "Too many requests"),

    /**
     * Conflict - client already disconnected or in transition.
     */
    CONFLICT(8, "Conflict");

    private final int code;
    private final String description;

    AdminCode(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Convert a numeric code to the corresponding AdminCode.
     *
     * @param code the numeric code
     * @return the AdminCode, or null if not found
     */
    public static AdminCode fromCode(int code) {
        for (AdminCode ac : values()) {
            if (ac.code == code) {
                return ac;
            }
        }
        return null;
    }
}