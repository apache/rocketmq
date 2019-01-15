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

import io.netty.channel.Channel;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;

public class ClientHousekeepingService implements ChannelEventListener {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ClientHousekeepingScheduledThread"));

    public ClientHousekeepingService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ClientHousekeepingService.this.scanExceptionChannel();
                } catch (Throwable e) {
                    log.error("Error occurred when scan not active client channels.", e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    private void scanExceptionChannel() {
        this.brokerController.getProducerManager().scanNotActiveChannel();
        this.brokerController.getConsumerManager().scanNotActiveChannel();
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void onChannelConnect(String remoteAddr, RemotingChannel channel) {
        log.info("Remoting channel connected: {}", RemotingHelper.parseChannelRemoteAddr(channel.remoteAddress()));
    }

    @Override
    public void onChannelClose(String remoteAddr, RemotingChannel remotingChannel) {
        log.info("Remoting channel closed: {}", RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()));
        NettyChannelImpl nettyChannel = (NettyChannelImpl) remotingChannel;
        Channel channel = nettyChannel.getChannel();
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, remotingChannel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, remotingChannel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }

    @Override
    public void onChannelException(String remoteAddr, RemotingChannel remotingChannel) {
        NettyChannelImpl nettyChannel = (NettyChannelImpl) remotingChannel;
        Channel channel = nettyChannel.getChannel();
        log.info("Remoting channel exception: {}", RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()));
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, remotingChannel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, remotingChannel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, RemotingChannel remotingChannel) {
        NettyChannelImpl nettyChannel = (NettyChannelImpl) remotingChannel;
        Channel channel = nettyChannel.getChannel();
        log.info("Remoting channel idle: {}", RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()));
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, remotingChannel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, remotingChannel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }
}
