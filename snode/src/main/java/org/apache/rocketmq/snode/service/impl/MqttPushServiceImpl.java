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
package org.apache.rocketmq.snode.service.impl;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.util.ReferenceCountUtil;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.netty.NettyChannelHandlerContextImpl;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.Client;
import org.apache.rocketmq.snode.client.impl.IOTClientManagerImpl;
import org.apache.rocketmq.snode.constant.SnodeConstant;
import org.apache.rocketmq.snode.util.MqttUtil;

public class MqttPushServiceImpl {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private SnodeController snodeController;
    private ExecutorService pushMqttMessageExecutorService;

    public MqttPushServiceImpl(final SnodeController snodeController) {
        this.snodeController = snodeController;
        pushMqttMessageExecutorService = ThreadUtils.newThreadPoolExecutor(
            this.snodeController.getMqttConfig().getPushMqttMessageMinPoolSize(),
            this.snodeController.getMqttConfig().getPushMqttMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(this.snodeController.getMqttConfig().getPushMqttMessageThreadPoolQueueCapacity()),
            "pushMqttMessageThread",
            false);
    }

    public class MqttPushTask implements Runnable {
        private AtomicBoolean canceled = new AtomicBoolean(false);
        private final ByteBuf message;
        private final String topic;
        private final Integer qos;
        private boolean retain;
        private Integer packetId;

        public MqttPushTask(final String topic, final ByteBuf message, final Integer qos, boolean retain,
            Integer packetId) {
            this.message = message;
            this.topic = topic;
            this.qos = qos;
            this.retain = retain;
            this.packetId = packetId;
        }

        @Override
        public void run() {
            if (!canceled.get()) {
                try {
                    RemotingCommand requestCommand = buildRequestCommand(topic, qos, retain, packetId);

                    //find those clients publishing the message to
                    IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) snodeController.getIotClientManager();
                    ConcurrentHashMap<String, ConcurrentHashMap<Client, Set<SubscriptionData>>> topic2SubscriptionTable = iotClientManager.getTopic2SubscriptionTable();
                    Set<Client> clients = new HashSet<>();
                    if (topic2SubscriptionTable.containsKey(MqttUtil.getRootTopic(topic))) {
                        ConcurrentHashMap<Client, Set<SubscriptionData>> client2SubscriptionDatas = topic2SubscriptionTable.get(MqttUtil.getRootTopic(topic));
                        for (Map.Entry<Client, Set<SubscriptionData>> entry : client2SubscriptionDatas.entrySet()) {
                            Set<SubscriptionData> subscriptionDatas = entry.getValue();
                            for (SubscriptionData subscriptionData : subscriptionDatas) {
                                if (MqttUtil.isMatch(subscriptionData.getTopic(), topic)) {
                                    clients.add(entry.getKey());
                                    break;
                                }
                            }
                        }
                    }
                    for (Client client : clients) {
                        RemotingChannel remotingChannel = client.getRemotingChannel();
                        if (client.getRemotingChannel() instanceof NettyChannelHandlerContextImpl) {
                            remotingChannel = new NettyChannelImpl(((NettyChannelHandlerContextImpl) client.getRemotingChannel()).getChannelHandlerContext().channel());
                        }
                        byte[] body = new byte[message.readableBytes()];
                        message.readBytes(body);
                        requestCommand.setBody(body);
                        snodeController.getMqttRemotingServer().push(remotingChannel, requestCommand, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
                    }
                } catch (Exception ex) {
                    log.warn("Exception was thrown when pushing MQTT message to topic: {}, exception={}", topic, ex.getMessage());
                } finally {
                    System.out.println("Release Bytebuf");
                    ReferenceCountUtil.release(message);
                }
            } else {
                log.info("Push message to topic: {} canceled!", topic);
            }
        }

        private RemotingCommand buildRequestCommand(final String topic, final Integer qos, boolean retain,
            Integer packetId) {
            MqttHeader mqttHeader = new MqttHeader();
            mqttHeader.setMessageType(MqttMessageType.PUBLISH.value());
            if (qos == 0) {
                mqttHeader.setDup(false);//DUP is always 0 for qos=0 messages
            } else {
                mqttHeader.setDup(false);//DUP is depending on whether it is a re-delivery of an earlier attempt.
            }
            mqttHeader.setQosLevel(qos);
            mqttHeader.setRetain(retain);
            mqttHeader.setPacketId(packetId);
            mqttHeader.setTopicName(topic);
            mqttHeader.setRemainingLength(4 + topic.getBytes().length + message.readableBytes());

            RemotingCommand pushMessage = RemotingCommand.createRequestCommand(RequestCode.MQTT_MESSAGE, mqttHeader);
            return pushMessage;
        }

        public void setCanceled(AtomicBoolean canceled) {
            this.canceled = canceled;
        }

    }

    public void pushMessageQos0(final String topic, final ByteBuf message) {
        MqttPushTask pushTask = new MqttPushTask(topic, message, 0, false, 0);
        pushMqttMessageExecutorService.submit(pushTask);
    }

    public void pushMessageQos1(final String topic, final ByteBuf message, final Integer qos, boolean retain,
        Integer packetId) {
        MqttPushTask pushTask = new MqttPushTask(topic, message, qos, retain, packetId);
        pushMqttMessageExecutorService.submit(pushTask);
    }

    public void shutdown() {
        this.pushMqttMessageExecutorService.shutdown();
    }
}
