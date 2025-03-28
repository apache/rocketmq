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
import io.netty.channel.ChannelFuture;
import java.lang.reflect.Field;
import java.util.Map;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProducerManagerTest {
    private ProducerManager producerManager;
    private String group = "FooBar";
    private ClientChannelInfo clientInfo;

    @Mock
    private Channel channel;

    @Before
    public void init() {
        producerManager = new ProducerManager();
        clientInfo = new ClientChannelInfo(channel, "clientId", LanguageCode.JAVA, 0);
    }

    @Test
    public void scanNotActiveChannel() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        AtomicReference<String> groupRef = new AtomicReference<>();
        AtomicReference<ClientChannelInfo> clientChannelInfoRef = new AtomicReference<>();
        producerManager.appendProducerChangeListener((event, group, clientChannelInfo) -> {
            switch (event) {
                case GROUP_UNREGISTER:
                    groupRef.set(group);
                    break;
                case CLIENT_UNREGISTER:
                    clientChannelInfoRef.set(clientChannelInfo);
                    break;
                default:
                    break;
            }
        });
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel)).isNotNull();
        assertThat(producerManager.findChannel("clientId")).isNotNull();
        Field field = ProducerManager.class.getDeclaredField("CHANNEL_EXPIRED_TIMEOUT");
        field.setAccessible(true);
        long channelExpiredTimeout = field.getLong(producerManager);
        clientInfo.setLastUpdateTimestamp(System.currentTimeMillis() - channelExpiredTimeout - 10);
        when(channel.close()).thenReturn(mock(ChannelFuture.class));
        producerManager.scanNotActiveChannel();
        assertThat(producerManager.getGroupChannelTable().get(group)).isNull();
        assertThat(groupRef.get()).isEqualTo(group);
        assertThat(clientChannelInfoRef.get()).isSameAs(clientInfo);
        assertThat(producerManager.findChannel("clientId")).isNull();
    }

    @Test
    public void scanNotActiveChannelWithSameClientId() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Channel channel1 = Mockito.mock(Channel.class);
        ClientChannelInfo clientInfo1 = new ClientChannelInfo(channel1, clientInfo.getClientId(), LanguageCode.JAVA, 0);
        producerManager.registerProducer(group, clientInfo1);
        AtomicReference<String> groupRef = new AtomicReference<>();
        AtomicReference<ClientChannelInfo> clientChannelInfoRef = new AtomicReference<>();
        producerManager.appendProducerChangeListener((event, group, clientChannelInfo) -> {
            switch (event) {
                case GROUP_UNREGISTER:
                    groupRef.set(group);
                    break;
                case CLIENT_UNREGISTER:
                    clientChannelInfoRef.set(clientChannelInfo);
                    break;
                default:
                    break;
            }
        });
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel)).isNotNull();
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel1)).isNotNull();
        assertThat(producerManager.findChannel("clientId")).isNotNull();
        Field field = ProducerManager.class.getDeclaredField("CHANNEL_EXPIRED_TIMEOUT");
        field.setAccessible(true);
        long channelExpiredTimeout = field.getLong(producerManager);
        clientInfo.setLastUpdateTimestamp(System.currentTimeMillis() - channelExpiredTimeout - 10);
        when(channel.close()).thenReturn(mock(ChannelFuture.class));
        producerManager.scanNotActiveChannel();
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel1)).isNotNull();
        assertThat(producerManager.findChannel("clientId")).isNotNull();
    }

    @Test
    public void doChannelCloseEvent() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        AtomicReference<String> groupRef = new AtomicReference<>();
        AtomicReference<ClientChannelInfo> clientChannelInfoRef = new AtomicReference<>();
        producerManager.appendProducerChangeListener((event, group, clientChannelInfo) -> {
            switch (event) {
                case GROUP_UNREGISTER:
                    groupRef.set(group);
                    break;
                case CLIENT_UNREGISTER:
                    clientChannelInfoRef.set(clientChannelInfo);
                    break;
                default:
                    break;
            }
        });
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel)).isNotNull();
        assertThat(producerManager.findChannel("clientId")).isNotNull();
        producerManager.doChannelCloseEvent("127.0.0.1", channel);
        assertThat(producerManager.getGroupChannelTable().get(group)).isNull();
        assertThat(groupRef.get()).isEqualTo(group);
        assertThat(clientChannelInfoRef.get()).isSameAs(clientInfo);
        assertThat(producerManager.findChannel("clientId")).isNull();
    }

    @Test
    public void testRegisterProducer() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Map<Channel, ClientChannelInfo> channelMap = producerManager.getGroupChannelTable().get(group);
        Channel channel1 = producerManager.findChannel("clientId");
        assertThat(channelMap).isNotNull();
        assertThat(channel1).isNotNull();
        assertThat(channelMap.get(channel)).isEqualTo(clientInfo);
        assertThat(channel1).isEqualTo(channel);
    }

    @Test
    public void testRegisterProducerWhenRegisterProducerIsNotEnabled() throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnableRegisterProducer(false);
        brokerConfig.setRejectTransactionMessage(true);
        ProducerManager producerManager = new ProducerManager(null, brokerConfig);

        producerManager.registerProducer(group, clientInfo);
        Map<Channel, ClientChannelInfo> channelMap = producerManager.getGroupChannelTable().get(group);
        Channel channel1 = producerManager.findChannel("clientId");
        assertThat(channelMap).isNull();
        assertThat(channel1).isNull();
    }

    @Test
    public void unregisterProducer() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        AtomicReference<String> groupRef = new AtomicReference<>();
        AtomicReference<ClientChannelInfo> clientChannelInfoRef = new AtomicReference<>();
        producerManager.appendProducerChangeListener((event, group, clientChannelInfo) -> {
            switch (event) {
                case GROUP_UNREGISTER:
                    groupRef.set(group);
                    break;
                case CLIENT_UNREGISTER:
                    clientChannelInfoRef.set(clientChannelInfo);
                    break;
                default:
                    break;
            }
        });
        Map<Channel, ClientChannelInfo> channelMap = producerManager.getGroupChannelTable().get(group);
        assertThat(channelMap).isNotNull();
        assertThat(channelMap.get(channel)).isEqualTo(clientInfo);
        Channel channel1 = producerManager.findChannel("clientId");
        assertThat(channel1).isNotNull();
        assertThat(channel1).isEqualTo(channel);
        producerManager.unregisterProducer(group, clientInfo);
        channelMap = producerManager.getGroupChannelTable().get(group);
        channel1 = producerManager.findChannel("clientId");
        assertThat(groupRef.get()).isEqualTo(group);
        assertThat(clientChannelInfoRef.get()).isSameAs(clientInfo);
        assertThat(channelMap).isNull();
        assertThat(channel1).isNull();

    }

    @Test
    public void testGetGroupChannelTable() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Map<Channel, ClientChannelInfo> oldMap = producerManager.getGroupChannelTable().get(group);
        
        producerManager.unregisterProducer(group, clientInfo);
        assertThat(oldMap.size()).isEqualTo(0);
    }

    @Test
    public void testGetAvailableChannel() {
        producerManager.registerProducer(group, clientInfo);

        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);
        Channel c = producerManager.getAvailableChannel(group);
        assertThat(c).isSameAs(channel);

        when(channel.isWritable()).thenReturn(false);
        c = producerManager.getAvailableChannel(group);
        assertThat(c).isSameAs(channel);

        when(channel.isActive()).thenReturn(false);
        c = producerManager.getAvailableChannel(group);
        assertThat(c).isNull();
    }

}