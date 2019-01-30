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

package org.apache.rocketmq.remoting.transport.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.util.internal.StringUtil;
import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.serialize.RemotingSerializable;

/**
 * Payload of {@link MqttConnectMessage}
 */
public final class RocketMQMqttConnectPayload extends RemotingSerializable {

    private String clientIdentifier;
    private String willTopic;
    private String willMessage;
    private String userName;
    private String password;

    public RocketMQMqttConnectPayload(
            String clientIdentifier,
            String willTopic,
            String willMessage,
            String userName,
            String password) {
        this.clientIdentifier = clientIdentifier;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
        this.userName = userName;
        this.password = password;
    }

    public static RocketMQMqttConnectPayload fromMqttConnectPayload(MqttConnectPayload payload) {
        return new RocketMQMqttConnectPayload(payload.clientIdentifier(), payload.willTopic(),
                payload.willMessage(), payload.userName(), payload.password());
    }

    public MqttConnectPayload toMqttConnectPayload() throws UnsupportedEncodingException {
        return new MqttConnectPayload(this.clientIdentifier, this.willTopic, this.willMessage.getBytes(
                RemotingUtil.REMOTING_CHARSET), this.userName, this.password.getBytes(RemotingUtil.REMOTING_CHARSET));
    }
    public String getClientIdentifier() {
        return clientIdentifier;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public String getWillMessage() {
        return willMessage;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public void setClientIdentifier(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
    }

    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public void setWillMessage(String willMessage) {
        this.willMessage = willMessage;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return new StringBuilder(StringUtil.simpleClassName(this))
            .append('[')
            .append("clientIdentifier=").append(clientIdentifier)
            .append(", willTopic=").append(willTopic)
            .append(", willMessage=").append(willMessage)
            .append(", userName=").append(userName)
            .append(", password=").append(password)
            .append(']')
            .toString();
    }
}
