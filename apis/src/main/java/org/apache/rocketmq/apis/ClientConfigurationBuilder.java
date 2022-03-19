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
 * Builder to set {@link ClientConfiguration}.
 */
public class ClientConfigurationBuilder {
    private String endpoints;
    private SessionCredentialsProvider sessionCredentialsProvider;
    private Duration requestTimeout;
    private boolean enableTracing;

    /**
     * Configure the endpoints with which the SDK should communicate.
     *
     * <p>Endpoints here means address of service, complying with the following scheme(part square brackets is
     * optional).
     * <p>1. DNS scheme(default): dns:host[:port], host is the host to resolve via DNS, port is the port to return
     * for each address. If not specified, 443 is used.
     * <p>2. ipv4 scheme: ipv4:address:port[,address:port,...]
     * <p>3. ipv6 scheme: ipv6:address:port[,address:port,...]
     * <p>4. http/https scheme: http|https://host[:port], similar to DNS scheme, if port not specified, 443 is used.
     *
     * @param endpoints address of service.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setEndpoints(String endpoints) {
        checkNotNull(endpoints, "endpoints should not be not null");
        this.endpoints = endpoints;
        return this;
    }

    public ClientConfigurationBuilder setCredentialProvider(SessionCredentialsProvider sessionCredentialsProvider) {
        this.sessionCredentialsProvider = checkNotNull(sessionCredentialsProvider, "credentialsProvider should not be "
            + "null");
        return this;
    }

    public ClientConfigurationBuilder setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = checkNotNull(requestTimeout, "requestTimeout should not be not null");
        return this;
    }

    public ClientConfigurationBuilder enableTracing(boolean enableTracing) {
        this.enableTracing = enableTracing;
        return this;
    }

    public ClientConfiguration build() {
        return new ClientConfiguration(endpoints, sessionCredentialsProvider, requestTimeout, enableTracing);
    }
}
