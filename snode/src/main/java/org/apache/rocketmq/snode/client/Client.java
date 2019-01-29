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
package org.apache.rocketmq.snode.client;

import java.util.Objects;
import java.util.Set;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.serialize.LanguageCode;
import org.apache.rocketmq.snode.client.impl.ClientRole;

public class Client {
    private ClientRole clientRole;

    private String clientId;

    private Set<String> groups;

    private RemotingChannel remotingChannel;

    private int heartbeatInterval;

    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    private int version;

    private LanguageCode language;

    private boolean isConnected;

    public ClientRole getClientRole() {
        return clientRole;
    }

    public void setClientRole(ClientRole clientRole) {
        this.clientRole = clientRole;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Client))
            return false;
        Client client = (Client) o;
        return
            version == client.version &&
                clientRole == client.clientRole &&
                Objects.equals(clientId, client.clientId) &&
                Objects.equals(groups, client.groups) &&
                Objects.equals(remotingChannel, client.remotingChannel) &&
                language == client.language &&
                isConnected == client.isConnected();
    }

    @Override public int hashCode() {
        return Objects.hash(clientRole, clientId, groups, remotingChannel, heartbeatInterval, lastUpdateTimestamp, version, language, isConnected);
    }

    public RemotingChannel getRemotingChannel() {
        return remotingChannel;
    }

    public void setRemotingChannel(RemotingChannel remotingChannel) {
        this.remotingChannel = remotingChannel;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    @Override public String toString() {
        return "Client{" +
            "clientRole=" + clientRole +
            ", clientId='" + clientId + '\'' +
            ", groups=" + groups +
            ", remotingChannel=" + remotingChannel +
            ", heartbeatInterval=" + heartbeatInterval +
            ", lastUpdateTimestamp=" + lastUpdateTimestamp +
            ", version=" + version +
            ", language=" + language +
            ", isConnected=" + isConnected +
            '}';
    }
}


