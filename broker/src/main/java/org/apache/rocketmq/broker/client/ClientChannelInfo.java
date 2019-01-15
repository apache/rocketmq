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
package org.apache.rocketmq.broker.client;

import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.serialize.LanguageCode;

public class ClientChannelInfo {
    private final RemotingChannel remotingChannel;
    private final String clientId;
    private final LanguageCode language;
    private final int version;
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ClientChannelInfo(RemotingChannel channel) {
        this(channel, null, null, 0);
    }

    public ClientChannelInfo(RemotingChannel remotingChannel, String clientId, LanguageCode language, int version) {
        this.remotingChannel = remotingChannel;
        this.clientId = clientId;
        this.language = language;
        this.version = version;
    }

    public RemotingChannel getRemotingChannel() {
        return remotingChannel;
    }

    public String getClientId() {
        return clientId;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public int getVersion() {
        return version;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((remotingChannel == null) ? 0 : remotingChannel.hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        result = prime * result + ((language == null) ? 0 : language.hashCode());
        result = prime * result + (int) (lastUpdateTimestamp ^ (lastUpdateTimestamp >>> 32));
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClientChannelInfo other = (ClientChannelInfo) obj;
        if (remotingChannel == null) {
            if (other.remotingChannel != null)
                return false;
        } else if (this.remotingChannel != other.remotingChannel) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "ClientChannelInfo [remotingChannel=" + remotingChannel + ", clientId=" + clientId + ", language=" + language
            + ", version=" + version + ", lastUpdateTimestamp=" + lastUpdateTimestamp + "]";
    }
}
