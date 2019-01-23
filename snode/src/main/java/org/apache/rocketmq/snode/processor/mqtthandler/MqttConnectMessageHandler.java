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

public class MqttConnectMessageHandler implements MessageHandler {

    private static final int MIN_AVAILABLE_VERSION = 3;
    private static final int MAX_AVAILABLE_VERSION = 4;

/*    private ClientManager clientManager;

    public MqttConnectMessageHandler(ClientManager clientManager) {
        this.clientManager = clientManager;
    }*/

    @Override public void handleMessage(MqttMessage message) {
//        MqttClient client = (MqttClient) message.getClient();
        if (!(message instanceof MqttConnectMessage)) {
            return;
        }
        MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) message;
        MqttConnectPayload payload = mqttConnectMessage.payload();

        MqttConnectReturnCode returnCode;
        MqttConnAckMessage ackMessage;

//        ChannelHandlerContext ctx = client.getCtx();

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
