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

package org.apache.rocketmq.mqtt.processor;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.client.ClientManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.exception.MQClientException;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.common.service.NnodeService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttMessageForwarder;
import org.apache.rocketmq.mqtt.service.WillMessageService;
import org.apache.rocketmq.mqtt.service.impl.MqttPushServiceImpl;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

import static io.netty.handler.codec.mqtt.MqttMessageType.PUBLISH;

public class InnerMqttMessageProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);

    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;
    private WillMessageService willMessageService;
    private MqttPushServiceImpl mqttPushService;
    private ClientManager iotClientManager;
    private RemotingServer innerMqttRemotingServer;
    private MqttConfig mqttConfig;
    private SnodeConfig snodeConfig;
    private EnodeService enodeService;
    private NnodeService nnodeService;
    private MqttMessageForwarder mqttMessageForwarder;

    public InnerMqttMessageProcessor(DefaultMqttMessageProcessor defaultMqttMessageProcessor, RemotingServer innerMqttRemotingServer) {
        this.defaultMqttMessageProcessor = defaultMqttMessageProcessor;
        this.willMessageService = this.defaultMqttMessageProcessor.getWillMessageService();
        this.mqttPushService = this.defaultMqttMessageProcessor.getMqttPushService();
        this.iotClientManager = this.defaultMqttMessageProcessor.getIotClientManager();
        this.innerMqttRemotingServer = innerMqttRemotingServer;
        this.enodeService = this.defaultMqttMessageProcessor.getEnodeService();
        this.nnodeService = this.defaultMqttMessageProcessor.getNnodeService();
        this.mqttMessageForwarder = new MqttMessageForwarder(this);
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel, RemotingCommand message)
        throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        MqttHeader mqttHeader = (MqttHeader) message.readCustomHeader();
        if(mqttHeader.getMessageType().equals(PUBLISH)){
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.valueOf(mqttHeader.getMessageType()),
                mqttHeader.isDup(), MqttQoS.valueOf(mqttHeader.getQosLevel()), mqttHeader.isRetain(),
                mqttHeader.getRemainingLength());
            MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(mqttHeader.getTopicName(), mqttHeader.getPacketId());
            MqttMessage mqttMessage = new MqttPublishMessage(fixedHeader, mqttPublishVariableHeader, Unpooled.copiedBuffer(message.getBody()));
            return mqttMessageForwarder.handleMessage(mqttMessage, remotingChannel);
        }else{
            return defaultMqttMessageProcessor.processRequest(remotingChannel, message);
        }

    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public WillMessageService getWillMessageService() {
        return willMessageService;
    }

    public MqttPushServiceImpl getMqttPushService() {
        return mqttPushService;
    }

    public ClientManager getIotClientManager() {
        return iotClientManager;
    }

    public MqttConfig getMqttConfig() {
        return mqttConfig;
    }

    public void setMqttConfig(MqttConfig mqttConfig) {
        this.mqttConfig = mqttConfig;
    }

    public SnodeConfig getSnodeConfig() {
        return snodeConfig;
    }

    public void setSnodeConfig(SnodeConfig snodeConfig) {
        this.snodeConfig = snodeConfig;
    }

    public EnodeService getEnodeService() {
        return enodeService;
    }

    public void setEnodeService(EnodeService enodeService) {
        this.enodeService = enodeService;
    }

    public NnodeService getNnodeService() {
        return nnodeService;
    }

    public void setNnodeService(NnodeService nnodeService) {
        this.nnodeService = nnodeService;
    }

    public DefaultMqttMessageProcessor getDefaultMqttMessageProcessor() {
        return defaultMqttMessageProcessor;
    }
}
