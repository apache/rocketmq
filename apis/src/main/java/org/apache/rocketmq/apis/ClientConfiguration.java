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

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;

/**
 * Common client configuration.
 */
public class ClientConfiguration {
    private final String endpoints;
    private final SessionCredentialsProvider sessionCredentialsProvider;
    private final Duration requestTimeout;
    private final boolean enableTracing;

    public static ClientConfigurationBuilder newBuilder() {
        return new ClientConfigurationBuilder();
    }

    public ClientConfiguration(String endpoints, SessionCredentialsProvider sessionCredentialsProvider,
        Duration requestTimeout, boolean enableTracing) {
        this.endpoints = checkNotNull(endpoints, "endpoints should not be null");
        this.sessionCredentialsProvider = checkNotNull(sessionCredentialsProvider, "credentialsProvider should not be"
            + " null");
        this.requestTimeout = checkNotNull(requestTimeout, "requestTimeout should be not null");
        this.enableTracing = enableTracing;
    }

    public String getEndpoints() {
        return endpoints;
    }

    public SessionCredentialsProvider getCredentialsProvider() {
        return sessionCredentialsProvider;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public boolean isEnableTracing() {
        return enableTracing;
    }
}
