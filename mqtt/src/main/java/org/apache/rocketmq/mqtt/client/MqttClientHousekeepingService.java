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
package org.apache.rocketmq.mqtt.client;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.ClientManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;

public class MqttClientHousekeepingService implements ChannelEventListener {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final ClientManager iotClientManager;

    public MqttClientHousekeepingService(final ClientManager iotClientManager) {
        this.iotClientManager = iotClientManager;
    }

    public void start(long interval) {
        this.iotClientManager.startScan(interval);
    }

    public void shutdown() {
        this.iotClientManager.shutdown();
    }

    private Client getClient(RemotingChannel remotingChannel) {
        if (remotingChannel instanceof NettyChannelImpl) {
            Channel channel = ((NettyChannelImpl) remotingChannel).getChannel();
            Attribute<Client> clientAttribute = channel.attr(MqttConstant.MQTT_CLIENT_ATTRIBUTE_KEY);
            if (clientAttribute != null) {
                Client client = clientAttribute.get();
                return client;
            }
        }
        log.warn("RemotingChannel type error: {}", remotingChannel.getClass());
        return null;
    }

    private void closeChannel(String remoteAddress, RemotingChannel remotingChannel) {
        Client client = getClient(remotingChannel);
        if (client != null) {
            switch (client.getClientRole()) {
                case IOTCLIENT:
                    this.iotClientManager.onClose(client.getGroups(), remotingChannel);
                    return;
                default:
            }
        }
        log.warn("Close channel without any role");
    }

    @Override
    public void onChannelConnect(String remoteAddr, RemotingChannel channel) {
        log.info("Remoting channel connected: {}", RemotingHelper.parseChannelRemoteAddr(channel.remoteAddress()));

    }

    @Override
    public void onChannelClose(String remoteAddr, RemotingChannel remotingChannel) {
        log.info("Remoting channel closed: {}", RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()));
        closeChannel(remoteAddr, remotingChannel);
    }

    @Override
    public void onChannelException(String remoteAddr, RemotingChannel remotingChannel) {
        log.info("Remoting channel exception: {}", RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()));
        closeChannel(remoteAddr, remotingChannel);

    }

    @Override
    public void onChannelIdle(String remoteAddr, RemotingChannel remotingChannel) {
        log.info("Remoting channel idle: {}", RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()));
        closeChannel(remoteAddr, remotingChannel);
    }
}
