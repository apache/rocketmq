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

package org.apache.rocketmq.apis.exception;

import com.google.common.base.MoreObjects;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Base exception for all exception raised in client, each exception should derive from current class.
 * It should throw exception which is derived from {@link ClientException} rather than {@link ClientException} itself.
 */
public abstract class ClientException extends Exception {
    /**
     * For those {@link ClientException} along with a remote procedure call, request id could be used to track the
     * request.
     */
    protected static final String REQUEST_ID_KEY = "request-id";

    private final ErrorClassification errorClassification;
    private final Map<String, String> context;

    ClientException(ErrorClassification errorClassification, String message, Throwable cause) {
        super(message, cause);
        this.errorClassification = errorClassification;
        this.context = new HashMap<>();
    }

    ClientException(ErrorClassification errorClassification, String message) {
        super(message);
        this.errorClassification = errorClassification;
        this.context = new HashMap<>();
    }

    @SuppressWarnings("SameParameterValue")
    protected void putMetadata(String key, String value) {
        context.put(key, value);
    }

    public Optional<String> getRequestId() {
        final String requestId = context.get(REQUEST_ID_KEY);
        return null == requestId ? Optional.empty() : Optional.of(requestId);
    }

    public ErrorClassification getErrorClassification() {
        return errorClassification;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(super.toString())
            .add("errorClassification", errorClassification)
            .add("context", context)
            .toString();
    }
}
