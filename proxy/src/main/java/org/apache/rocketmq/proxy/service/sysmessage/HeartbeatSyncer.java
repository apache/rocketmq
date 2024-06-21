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

package org.apache.rocketmq.proxy.service.sysmessage;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class HeartbeatSyncer extends AbstractSystemMessageSyncer {

    protected ThreadPoolExecutor threadPoolExecutor;
    protected ConsumerManager consumerManager;
    protected final Map<String /* group @ channelId as longText */, RemoteChannel> remoteChannelMap = new ConcurrentHashMap<>();
    protected String localProxyId;

    public HeartbeatSyncer(TopicRouteService topicRouteService, AdminService adminService,
                           ConsumerManager consumerManager, MQClientAPIFactory mqClientAPIFactory, RPCHook rpcHook) {
        super(topicRouteService, adminService, mqClientAPIFactory, rpcHook);
        this.consumerManager = consumerManager;
        this.localProxyId = buildLocalProxyId();
        this.init();
    }

    protected void init() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.threadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getHeartbeatSyncerThreadPoolNums(),
            proxyConfig.getHeartbeatSyncerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "HeartbeatSyncer",
            proxyConfig.getHeartbeatSyncerThreadPoolQueueCapacity()
        );
        this.consumerManager.appendConsumerIdsChangeListener(new ConsumerIdsChangeListener() {
            @Override
            public void handle(ConsumerGroupEvent event, String group, Object... args) {
                processConsumerGroupEvent(event, group, args);
            }

            @Override
            public void shutdown() {

            }
        });
    }

    @Override
    public void shutdown() throws Exception {
        this.threadPoolExecutor.shutdown();
        super.shutdown();
    }

    protected void processConsumerGroupEvent(ConsumerGroupEvent event, String group, Object... args) {
        if (event == ConsumerGroupEvent.CLIENT_UNREGISTER) {
            if (args == null || args.length < 1) {
                return;
            }
            if (args[0] instanceof ClientChannelInfo) {
                ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                remoteChannelMap.remove(buildKey(group, clientChannelInfo.getChannel()));
            }
        }
    }

    public void onConsumerRegister(String consumerGroup, ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList) {
        if (clientChannelInfo == null || ChannelHelper.isRemote(clientChannelInfo.getChannel())) {
            return;
        }
        try {
            this.threadPoolExecutor.submit(() -> {
                try {
                    RemoteChannel remoteChannel = RemoteChannel.create(clientChannelInfo.getChannel());
                    if (remoteChannel == null) {
                        return;
                    }
                    HeartbeatSyncerData data = new HeartbeatSyncerData(
                        HeartbeatType.REGISTER,
                        clientChannelInfo.getClientId(),
                        clientChannelInfo.getLanguage(),
                        clientChannelInfo.getVersion(),
                        consumerGroup,
                        consumeType,
                        messageModel,
                        consumeFromWhere,
                        localProxyId,
                        remoteChannel.encode()
                    );
                    data.setSubscriptionDataSet(subList);

                    log.debug("sync register heart beat. topic:{}, data:{}", this.getBroadcastTopicName(), data);
                    this.sendSystemMessage(data);
                } catch (Throwable t) {
                    log.error("heartbeat register broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}, messageModel:{}, consumeFromWhere:{}, subList:{}",
                        consumerGroup, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList, t);
                }
            });
        } catch (Throwable t) {
            log.error("heartbeat submit register broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}, messageModel:{}, consumeFromWhere:{}, subList:{}",
                consumerGroup, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList, t);
        }
    }

    public void onConsumerUnRegister(String consumerGroup, ClientChannelInfo clientChannelInfo) {
        if (clientChannelInfo == null || ChannelHelper.isRemote(clientChannelInfo.getChannel())) {
            return;
        }
        try {
            this.threadPoolExecutor.submit(() -> {
                try {
                    RemoteChannel remoteChannel = RemoteChannel.create(clientChannelInfo.getChannel());
                    if (remoteChannel == null) {
                        return;
                    }
                    HeartbeatSyncerData data = new HeartbeatSyncerData(
                        HeartbeatType.UNREGISTER,
                        clientChannelInfo.getClientId(),
                        clientChannelInfo.getLanguage(),
                        clientChannelInfo.getVersion(),
                        consumerGroup,
                        null,
                        null,
                        null,
                        localProxyId,
                        remoteChannel.encode()
                    );

                    log.debug("sync unregister heart beat. topic:{}, data:{}", this.getBroadcastTopicName(), data);
                    this.sendSystemMessage(data);
                } catch (Throwable t) {
                    log.error("heartbeat unregister broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}",
                        consumerGroup, clientChannelInfo, t);
                }
            });
        } catch (Throwable t) {
            log.error("heartbeat submit unregister broadcast failed. group:{}, clientChannelInfo:{}, consumeType:{}",
                consumerGroup, clientChannelInfo, t);
        }
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (msgs == null || msgs.isEmpty()) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        for (MessageExt msg : msgs) {
            try {
                HeartbeatSyncerData data = JSON.parseObject(new String(msg.getBody(), StandardCharsets.UTF_8), HeartbeatSyncerData.class);
                if (data.getLocalProxyId().equals(localProxyId)) {
                    continue;
                }

                RemoteChannel decodedChannel = RemoteChannel.decode(data.getChannelData());
                RemoteChannel channel = remoteChannelMap.computeIfAbsent(buildKey(data.getGroup(), decodedChannel), key -> decodedChannel);
                channel.setExtendAttribute(decodedChannel.getChannelExtendAttribute());
                ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                    channel,
                    data.getClientId(),
                    data.getLanguage(),
                    data.getVersion()
                );
                log.debug("start process remote channel. data:{}, clientChannelInfo:{}", data, clientChannelInfo);
                if (data.getHeartbeatType().equals(HeartbeatType.REGISTER)) {
                    this.consumerManager.registerConsumer(
                        data.getGroup(),
                        clientChannelInfo,
                        data.getConsumeType(),
                        data.getMessageModel(),
                        data.getConsumeFromWhere(),
                        data.getSubscriptionDataSet(),
                        false
                    );
                } else {
                    this.consumerManager.unregisterConsumer(
                        data.getGroup(),
                        clientChannelInfo,
                        false
                    );
                }
            } catch (Throwable t) {
                log.error("heartbeat consume message failed. msg:{}, data:{}", msg, new String(msg.getBody(), StandardCharsets.UTF_8), t);
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private String buildLocalProxyId() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        // use local address, remoting port and grpc port to build unique local proxy Id
        return proxyConfig.getLocalServeAddr() + "%" + proxyConfig.getRemotingListenPort() + "%" + proxyConfig.getGrpcServerPort();
    }

    private static String buildKey(String group, Channel channel) {
        return group + "@" + channel.id().asLongText();
    }
}
