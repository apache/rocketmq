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
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.client.ClientManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.exception.MQClientException;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.common.service.NnodeService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MqttClientHousekeepingService;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttConnectMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttDisconnectMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPingreqMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPubackMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPubcompMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPublishMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPubrecMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPubrelMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttSubscribeMessageHandler;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttUnsubscribeMessagHandler;
import org.apache.rocketmq.mqtt.service.WillMessageService;
import org.apache.rocketmq.mqtt.service.impl.MqttPushServiceImpl;
import org.apache.rocketmq.mqtt.service.impl.WillMessageServiceImpl;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.remoting.util.MqttEncodeDecodeUtil;

public class DefaultMqttMessageProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);

    private Map<MqttMessageType, MessageHandler> type2handler = new HashMap<>();
    private static final int MIN_AVAILABLE_VERSION = 3;
    private static final int MAX_AVAILABLE_VERSION = 4;
    private WillMessageService willMessageService;
    private MqttPushServiceImpl mqttPushService;
    private ClientManager iotClientManager;
    private RemotingServer mqttRemotingServer;
    private MqttClientHousekeepingService mqttClientHousekeepingService;
    private MqttConfig mqttConfig;
    private SnodeConfig snodeConfig;
    private EnodeService enodeService;
    private NnodeService nnodeService;

    public DefaultMqttMessageProcessor(MqttConfig mqttConfig, SnodeConfig snodeConfig,
        RemotingServer mqttRemotingServer,
        EnodeService enodeService, NnodeService nnodeService) {
        this.mqttConfig = mqttConfig;
        this.snodeConfig = snodeConfig;
        this.willMessageService = new WillMessageServiceImpl();
        this.mqttPushService = new MqttPushServiceImpl(this, mqttConfig);
        this.iotClientManager = new IOTClientManagerImpl();
        this.mqttRemotingServer = mqttRemotingServer;
        this.enodeService = enodeService;
        this.nnodeService = nnodeService;
        this.mqttClientHousekeepingService = new MqttClientHousekeepingService(iotClientManager);
        this.mqttClientHousekeepingService.start(mqttConfig.getHouseKeepingInterval());

        registerMessageHandler(MqttMessageType.CONNECT,
            new MqttConnectMessageHandler(this));
        registerMessageHandler(MqttMessageType.DISCONNECT,
            new MqttDisconnectMessageHandler(this));
        registerMessageHandler(MqttMessageType.PINGREQ,
            new MqttPingreqMessageHandler(this));
        registerMessageHandler(MqttMessageType.PUBLISH,
            new MqttPublishMessageHandler(this));
        registerMessageHandler(MqttMessageType.PUBACK, new MqttPubackMessageHandler(this));
        registerMessageHandler(MqttMessageType.PUBCOMP,
            new MqttPubcompMessageHandler(this));
        registerMessageHandler(MqttMessageType.PUBREC, new MqttPubrecMessageHandler(this));
        registerMessageHandler(MqttMessageType.PUBREL, new MqttPubrelMessageHandler(this));
        registerMessageHandler(MqttMessageType.SUBSCRIBE,
            new MqttSubscribeMessageHandler(this));
        registerMessageHandler(MqttMessageType.UNSUBSCRIBE,
            new MqttUnsubscribeMessagHandler(this));
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel, RemotingCommand message)
        throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        MqttHeader mqttHeader = (MqttHeader) message.readCustomHeader();
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.valueOf(mqttHeader.getMessageType()),
            mqttHeader.isDup(), MqttQoS.valueOf(mqttHeader.getQosLevel()), mqttHeader.isRetain(),
            mqttHeader.getRemainingLength());
        MqttMessage mqttMessage = null;
        switch (fixedHeader.messageType()) {
            case CONNECT:
                MqttConnectVariableHeader mqttConnectVariableHeader = new MqttConnectVariableHeader(
                    mqttHeader.getName(), mqttHeader.getVersion(), mqttHeader.isHasUserName(),
                    mqttHeader.isHasPassword(), mqttHeader.isWillRetain(),
                    mqttHeader.getWillQos(), mqttHeader.isWillFlag(),
                    mqttHeader.isCleanSession(), mqttHeader.getKeepAliveTimeSeconds());
                MqttConnectPayload mqttConnectPayload = (MqttConnectPayload) MqttEncodeDecodeUtil.decode(message.getBody(), MqttConnectPayload.class);
                mqttMessage = new MqttConnectMessage(fixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
                break;
            case PUBLISH:
                MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(mqttHeader.getTopicName(), mqttHeader.getPacketId());
                mqttMessage = new MqttPublishMessage(fixedHeader, mqttPublishVariableHeader, Unpooled.copiedBuffer(message.getBody()));
                break;
            case SUBSCRIBE:
                MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(mqttHeader.getMessageId());
                MqttSubscribePayload mqttSubscribePayload = (MqttSubscribePayload) MqttEncodeDecodeUtil.decode(message.getBody(), MqttSubscribePayload.class);
                mqttMessage = new MqttSubscribeMessage(fixedHeader, mqttMessageIdVariableHeader, mqttSubscribePayload);
                break;
            case UNSUBSCRIBE:
            case PINGREQ:
                break;
            case DISCONNECT:
        }
        return type2handler.get(MqttMessageType.valueOf(mqttHeader.getMessageType())).handleMessage(mqttMessage, remotingChannel);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private void registerMessageHandler(MqttMessageType type, MessageHandler handler) {
        type2handler.put(type, handler);
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

    public RemotingServer getMqttRemotingServer() {
        return mqttRemotingServer;
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
}
