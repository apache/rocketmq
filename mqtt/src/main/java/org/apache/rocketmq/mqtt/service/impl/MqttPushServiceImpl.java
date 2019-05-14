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
package org.apache.rocketmq.mqtt.service.impl;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.mqtt.service.MqttPushService;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.netty.NettyChannelHandlerContextImpl;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttPushServiceImpl implements MqttPushService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);

    private ExecutorService pushMqttMessageExecutorService;
    private static DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttPushServiceImpl(DefaultMqttMessageProcessor defaultMqttMessageProcessor, MqttConfig mqttConfig) {
        this.defaultMqttMessageProcessor = defaultMqttMessageProcessor;
        pushMqttMessageExecutorService = ThreadUtils.newThreadPoolExecutor(
            mqttConfig.getPushMqttMessageMinPoolSize(),
            mqttConfig.getPushMqttMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(mqttConfig.getPushMqttMessageThreadPoolQueueCapacity()),
            "pushMqttMessageThread",
            false);
    }

    public static class MqttPushTask implements Runnable {
        private AtomicBoolean canceled = new AtomicBoolean(false);
        private final ByteBuf message;
        private final MqttHeader mqttHeader;
        private Client client;
//        private final String topic;
//        private final Integer qos;
//        private boolean retain;
//        private Integer packetId;

        public MqttPushTask(final MqttHeader mqttHeader, final ByteBuf message, Client client) {
            this.message = message;
            this.mqttHeader = mqttHeader;
//            this.topic = topic;
//            this.qos = qos;
//            this.retain = retain;
//            this.packetId = packetId;
            this.client = client;
        }

        @Override
        public void run() {
            if (!canceled.get()) {
                try {
                    RemotingCommand requestCommand = buildRequestCommand(this.mqttHeader);

                    RemotingChannel remotingChannel = client.getRemotingChannel();
                    if (client.getRemotingChannel() instanceof NettyChannelHandlerContextImpl) {
                        remotingChannel = new NettyChannelImpl(((NettyChannelHandlerContextImpl) client.getRemotingChannel()).getChannelHandlerContext().channel());
                    }
                    byte[] body = new byte[message.readableBytes()];
                    message.readBytes(body);
                    requestCommand.setBody(body);
                    defaultMqttMessageProcessor.getMqttRemotingServer().push(remotingChannel, requestCommand, MqttConstant.DEFAULT_TIMEOUT_MILLS);
                } catch (Exception ex) {
                    log.warn("Exception was thrown when pushing MQTT message to topic: {}, clientId:{}, exception={}", mqttHeader.getTopicName(), client.getClientId(), ex.getMessage());
                } finally {
                    ReferenceCountUtil.release(message);
                }
            } else {
                log.info("Push message to topic: {}, clientId:{}, canceled!", mqttHeader.getTopicName(), client.getClientId());
            }
        }

        private RemotingCommand buildRequestCommand(MqttHeader mqttHeader) {
//            if (qos == 0) {
//                mqttHeader.setDup(false);//DUP is always 0 for qos=0 messages
//            } else {
//                mqttHeader.setDup(false);//DUP is depending on whether it is a re-delivery of an earlier attempt.
//            }
//            mqttHeader.setRemainingLength(4 + topic.getBytes().length + message.readableBytes());

            RemotingCommand pushMessage = RemotingCommand.createRequestCommand(RequestCode.MQTT_MESSAGE, mqttHeader);
            return pushMessage;
        }

        public void setCanceled(AtomicBoolean canceled) {
            this.canceled = canceled;
        }

    }

    public void pushMessageQos(MqttHeader mqttHeader, final ByteBuf message, Client client) {
        MqttPushTask pushTask = new MqttPushTask(mqttHeader, message, client);
        pushMqttMessageExecutorService.submit(pushTask);
    }

    public void shutdown() {
        this.pushMqttMessageExecutorService.shutdown();
    }
}
