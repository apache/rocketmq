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

import java.util.Optional;

/**
 * Session credentials used in service authentications.
 */
public class SessionCredentials {
    private final String accessKey;
    private final String accessSecret;
    private final String securityToken;

    public SessionCredentials(String accessKey, String accessSecret, String securityToken) {
        this.accessKey = checkNotNull(accessKey, "accessKey should not be null");
        this.accessSecret = checkNotNull(accessSecret, "accessSecret should not be null");
        this.securityToken = checkNotNull(securityToken, "securityToken should not be null");
    }

    public SessionCredentials(String accessKey, String accessSecret) {
        this.accessKey = checkNotNull(accessKey, "accessKey should not be null");
        this.accessSecret = checkNotNull(accessSecret, "accessSecret should not be null");
        this.securityToken = null;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getAccessSecret() {
        return accessSecret;
    }

    public Optional<String> tryGetSecurityToken() {
        return null == securityToken ? Optional.empty() : Optional.of(securityToken);
    }
}
