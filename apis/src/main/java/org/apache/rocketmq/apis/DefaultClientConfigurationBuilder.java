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
 * Default builder to set {@link DefaultClientConfiguration}.
 */
public class DefaultClientConfigurationBuilder implements ClientConfigurationBuilder {
    private String endpoints;
    private SessionCredentialsProvider sessionCredentialsProvider;
    private Duration requestTimeout;
    private boolean enableTracing;


    @Override
    public DefaultClientConfigurationBuilder endpoints(String endpoints) {
        checkNotNull(endpoints, "endpoints should not be not null");
        this.endpoints = endpoints;
        return this;
    }

    @Override
    public DefaultClientConfigurationBuilder sessionCredentialsProvider(SessionCredentialsProvider sessionCredentialsProvider) {
        this.sessionCredentialsProvider = checkNotNull(sessionCredentialsProvider, "credentialsProvider should not be "
            + "null");
        return this;
    }

    @Override
    public DefaultClientConfigurationBuilder requestTimeout(Duration requestTimeout) {
        this.requestTimeout = checkNotNull(requestTimeout, "requestTimeout should not be not null");
        return this;
    }

    @Override
    public DefaultClientConfigurationBuilder enableTracing(boolean enableTracing) {
        this.enableTracing = enableTracing;
        return this;
    }

    public DefaultClientConfiguration build() {
        return new DefaultClientConfiguration(endpoints, sessionCredentialsProvider, requestTimeout, enableTracing);
    }
}
