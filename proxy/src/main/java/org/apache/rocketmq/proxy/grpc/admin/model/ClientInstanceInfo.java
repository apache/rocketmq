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

package org.apache.rocketmq.proxy.grpc.admin.model;

import java.util.List;

/**
 * Client instance basic information model.
 * Maps to the ClientInstance proto message.
 */
public class ClientInstanceInfo {
    private String clientId;
    private String language;
    private String clientVersion;
    private String protocol;
    private String accessPoint;
    private long connectAt;
    private long lastActiveAt;
    private String role;
    private String group;
    private List<String> topics;

    public ClientInstanceInfo() {
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getClientVersion() {
        return clientVersion;
    }

    public void setClientVersion(String clientVersion) {
        this.clientVersion = clientVersion;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getAccessPoint() {
        return accessPoint;
    }

    public void setAccessPoint(String accessPoint) {
        this.accessPoint = accessPoint;
    }

    public long getConnectAt() {
        return connectAt;
    }

    public void setConnectAt(long connectAt) {
        this.connectAt = connectAt;
    }

    public long getLastActiveAt() {
        return lastActiveAt;
    }

    public void setLastActiveAt(long lastActiveAt) {
        this.lastActiveAt = lastActiveAt;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    @Override
    public String toString() {
        return "ClientInstanceInfo{" +
            "clientId='" + clientId + '\'' +
            ", language='" + language + '\'' +
            ", clientVersion='" + clientVersion + '\'' +
            ", protocol='" + protocol + '\'' +
            ", accessPoint='" + accessPoint + '\'' +
            ", connectAt=" + connectAt +
            ", lastActiveAt=" + lastActiveAt +
            ", role='" + role + '\'' +
            ", group='" + group + '\'' +
            ", topics=" + topics +
            '}';
    }
}