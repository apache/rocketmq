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
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
        clientInfo = new ClientChannelInfo(channel);
    }

    @Test
    public void scanNotActiveChannel() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel)).isNotNull();

        Field field = ProducerManager.class.getDeclaredField("CHANNEL_EXPIRED_TIMEOUT");
        field.setAccessible(true);
        long CHANNEL_EXPIRED_TIMEOUT = field.getLong(producerManager);
        clientInfo.setLastUpdateTimestamp(System.currentTimeMillis() - CHANNEL_EXPIRED_TIMEOUT - 10);
        when(channel.close()).thenReturn(mock(ChannelFuture.class));
        producerManager.scanNotActiveChannel();
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel)).isNull();
    }

    @Test
    public void doChannelCloseEvent() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel)).isNotNull();
        producerManager.doChannelCloseEvent("127.0.0.1", channel);
        assertThat(producerManager.getGroupChannelTable().get(group).get(channel)).isNull();
    }

    @Test
    public void testRegisterProducer() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        HashMap<Channel, ClientChannelInfo> channelMap = producerManager.getGroupChannelTable().get(group);
        assertThat(channelMap).isNotNull();
        assertThat(channelMap.get(channel)).isEqualTo(clientInfo);
    }

    @Test
    public void unregisterProducer() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        HashMap<Channel, ClientChannelInfo> channelMap = producerManager.getGroupChannelTable().get(group);
        assertThat(channelMap).isNotNull();
        assertThat(channelMap.get(channel)).isEqualTo(clientInfo);

        producerManager.unregisterProducer(group, clientInfo);
        channelMap = producerManager.getGroupChannelTable().get(group);
        assertThat(channelMap).isNull();
    }

}