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

package org.apache.rocketmq.apis;

import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Base exception for all exception raised in client, each exception should derive from current class.
 * It should throw exception which is derived from {@link ClientException} rather than {@link ClientException} itself.
 */
public class ClientException extends Exception {
    /**
     * For those {@link ClientException} along with a remote procedure call, request id could be used to track the
     * request.
     */
    protected static final String REQUEST_ID_KEY = "request-id";
    protected static final String RESPONSE_CODE_KEY = "response-code";

    private final Map<String, String> context;
    private final List<Throwable> throwableList;

    public ClientException(String message, Throwable cause) {
        super(message, cause);
        this.context = new HashMap<>();
        this.throwableList = new ArrayList<>();
    }

    public ClientException(String message) {
        super(message);
        this.context = new HashMap<>();
        this.throwableList = new ArrayList<>();
    }

    public ClientException(Throwable t) {
        super(t);
        this.context = new HashMap<>();
        this.throwableList = new ArrayList<>();
    }

    public ClientException(int responseCode, String message) {
        this(message);
        putMetadata(RESPONSE_CODE_KEY, String.valueOf(responseCode));
    }

    public ClientException(Throwable... throwableList) {
        this.context = new HashMap<>();
        this.throwableList = new ArrayList<>();
        this.throwableList.addAll(Arrays.stream(throwableList).collect(Collectors.toList()));
    }

    @SuppressWarnings("SameParameterValue")
    protected void putMetadata(String key, String value) {
        context.put(key, value);
    }

    public Optional<String> getRequestId() {
        final String requestId = context.get(REQUEST_ID_KEY);
        return null == requestId ? Optional.empty() : Optional.of(requestId);
    }

    public Optional<String> getResponseCode() {
        final String responseCode = context.get(RESPONSE_CODE_KEY);
        return null == responseCode ? Optional.empty() : Optional.of(responseCode);
    }

    @Override
    public String toString() {
        final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(super.toString());
        if (!context.isEmpty()) {
            helper.add("context", context);
        }
        if (!throwableList.isEmpty()) {
            helper.add("throwableList", throwableList);
        }
        return helper.toString();
    }
}
