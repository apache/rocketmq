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

package org.apache.rocketmq.acl.common;

import com.google.common.base.MoreObjects;

public class AuthenticationHeader {
    private String remoteAddress;
    private String tenantId;
    private String namespace;
    private String authorization;
    private String datetime;
    private String sessionToken;
    private String requestId;
    private String language;
    private String clientVersion;
    private String protocol;
    private int requestCode;

    AuthenticationHeader(final String remoteAddress, final String tenantId, final String namespace,
        final String authorization, final String datetime, final String sessionToken, final String requestId,
        final String language, final String clientVersion, final String protocol, final int requestCode) {
        this.remoteAddress = remoteAddress;
        this.tenantId = tenantId;
        this.namespace = namespace;
        this.authorization = authorization;
        this.datetime = datetime;
        this.sessionToken = sessionToken;
        this.requestId = requestId;
        this.language = language;
        this.clientVersion = clientVersion;
        this.protocol = protocol;
        this.requestCode = requestCode;
    }

    public static class MetadataHeaderBuilder {
        private String remoteAddress;
        private String tenantId;
        private String namespace;
        private String authorization;
        private String datetime;
        private String sessionToken;
        private String requestId;
        private String language;
        private String clientVersion;
        private String protocol;
        private int requestCode;

        MetadataHeaderBuilder() {
        }

        public AuthenticationHeader.MetadataHeaderBuilder remoteAddress(final String remoteAddress) {
            this.remoteAddress = remoteAddress;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder tenantId(final String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder namespace(final String namespace) {
            this.namespace = namespace;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder authorization(final String authorization) {
            this.authorization = authorization;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder datetime(final String datetime) {
            this.datetime = datetime;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder sessionToken(final String sessionToken) {
            this.sessionToken = sessionToken;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder requestId(final String requestId) {
            this.requestId = requestId;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder language(final String language) {
            this.language = language;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder clientVersion(final String clientVersion) {
            this.clientVersion = clientVersion;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder protocol(final String protocol) {
            this.protocol = protocol;
            return this;
        }

        public AuthenticationHeader.MetadataHeaderBuilder requestCode(final int requestCode) {
            this.requestCode = requestCode;
            return this;
        }

        public AuthenticationHeader build() {
            return new AuthenticationHeader(this.remoteAddress, this.tenantId, this.namespace, this.authorization,
                this.datetime, this.sessionToken, this.requestId, this.language, this.clientVersion, this.protocol,
                this.requestCode);
        }
    }

    public static AuthenticationHeader.MetadataHeaderBuilder builder() {
        return new AuthenticationHeader.MetadataHeaderBuilder();
    }

    public String getRemoteAddress() {
        return this.remoteAddress;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public String getAuthorization() {
        return this.authorization;
    }

    public String getDatetime() {
        return this.datetime;
    }

    public String getSessionToken() {
        return this.sessionToken;
    }

    public String getRequestId() {
        return this.requestId;
    }

    public String getLanguage() {
        return this.language;
    }

    public String getClientVersion() {
        return this.clientVersion;
    }

    public String getProtocol() {
        return this.protocol;
    }

    public int getRequestCode() {
        return this.requestCode;
    }

    public void setRemoteAddress(final String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public void setTenantId(final String tenantId) {
        this.tenantId = tenantId;
    }

    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    public void setAuthorization(final String authorization) {
        this.authorization = authorization;
    }

    public void setDatetime(final String datetime) {
        this.datetime = datetime;
    }

    public void setSessionToken(final String sessionToken) {
        this.sessionToken = sessionToken;
    }

    public void setRequestId(final String requestId) {
        this.requestId = requestId;
    }

    public void setLanguage(final String language) {
        this.language = language;
    }

    public void setClientVersion(final String clientVersion) {
        this.clientVersion = clientVersion;
    }

    public void setProtocol(final String protocol) {
        this.protocol = protocol;
    }

    public void setRequestCode(int requestCode) {
        this.requestCode = requestCode;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("remoteAddress", remoteAddress)
            .add("tenantId", tenantId)
            .add("namespace", namespace)
            .add("authorization", authorization)
            .add("datetime", datetime)
            .add("sessionToken", sessionToken)
            .add("requestId", requestId)
            .add("language", language)
            .add("clientVersion", clientVersion)
            .add("protocol", protocol)
            .add("requestCode", requestCode)
            .toString();
    }
}
