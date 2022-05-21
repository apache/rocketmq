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

import java.time.Duration;
import java.util.Optional;

/**
 * Common client configuration.
 */
public class ClientConfiguration {
    private final String accessPoint;
    private final SessionCredentialsProvider sessionCredentialsProvider;
    private final Duration requestTimeout;

    public static ClientConfigurationBuilder newBuilder() {
        return new ClientConfigurationBuilder();
    }

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    ClientConfiguration(String accessPoint, SessionCredentialsProvider sessionCredentialsProvider, Duration requestTimeout) {
        this.accessPoint = accessPoint;
        this.sessionCredentialsProvider = sessionCredentialsProvider;
        this.requestTimeout = requestTimeout;
    }

    public String getAccessPoint() {
        return accessPoint;
    }

    public Optional<SessionCredentialsProvider> getCredentialsProvider() {
        return null == sessionCredentialsProvider ? Optional.empty() : Optional.of(sessionCredentialsProvider);
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }
}
