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

package org.apache.rocketmq.snode.processor.mqtthandler;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.Client;
import org.apache.rocketmq.snode.client.ClientManager;
import org.apache.rocketmq.snode.client.impl.IOTClientManagerImpl;

public class MqttConnectMessageHandler implements MessageHandler {

    private final SnodeController snodeController;
    private static final int MIN_AVAILABLE_VERSION = 3;
    private static final int MAX_AVAILABLE_VERSION = 4;

    public MqttConnectMessageHandler(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        if (!(message instanceof MqttConnectMessage)) {
            return null;
        }
        MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) message;
        MqttConnectPayload payload = mqttConnectMessage.payload();

        MqttConnectReturnCode returnCode;
        MqttConnAckMessage ackMessage;

        if (isConnected(remotingChannel, mqttConnectMessage.payload().clientIdentifier())) {

        }
//        ChannelHandlerContext ctx = client.getCtx();
        return null;
    }

    private boolean isConnected(RemotingChannel remotingChannel, String clientId) {
        ClientManager iotClientManager = snodeController.getIotClientManager();
        Client client = iotClientManager.getClient(IOTClientManagerImpl.IOTGROUP, remotingChannel);
        if (client != null && client.getClientId().equals(clientId) && client.isConnected()) {
            return true;
        }
        return false;
    }

    private boolean isServiceAviable(MqttConnectMessage connectMessage) {
        int version = connectMessage.variableHeader().version();
        return version >= MIN_AVAILABLE_VERSION && version <= MAX_AVAILABLE_VERSION;
    }

    private boolean checkPassword(byte[] bytes) {
        return true;
    }

    private boolean checkUsername(String s) {
        return true;
    }

    private boolean isAuthorized(MqttConnectMessage message) {
        return true;
    }

    private boolean isClientIdValid(String s) {
        return true;
    }
}
