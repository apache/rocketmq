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

package org.apache.rocketmq.proxy.remoting.channel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import java.util.HashSet;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.remoting.RemotingProxyOutClient;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class RemotingChannelManagerTest {
    @Mock
    private RemotingProxyOutClient remotingProxyOutClient;
    @Mock
    private ProxyRelayService proxyRelayService;

    private final String remoteAddress = "10.152.39.53:9768";
    private final String localAddress = "11.193.0.1:1210";
    private RemotingChannelManager remotingChannelManager;
    private final ProxyContext ctx = ProxyContext.createForInner(this.getClass());

    @Before
    public void before() {
        this.remotingChannelManager = new RemotingChannelManager(this.remotingProxyOutClient, this.proxyRelayService);
    }

    @Test
    public void testCreateChannel() {
        String group = "group";
        String clientId = RandomStringUtils.randomAlphabetic(10);

        Channel producerChannel = createMockChannel();
        RemotingChannel producerRemotingChannel = this.remotingChannelManager.createProducerChannel(ctx, producerChannel, group, clientId);
        assertNotNull(producerRemotingChannel);
        assertSame(producerRemotingChannel, this.remotingChannelManager.createProducerChannel(ctx, producerChannel, group, clientId));

        Channel consumerChannel = createMockChannel();
        RemotingChannel consumerRemotingChannel = this.remotingChannelManager.createConsumerChannel(ctx, consumerChannel, group, clientId, new HashSet<>());
        assertSame(consumerRemotingChannel, this.remotingChannelManager.createConsumerChannel(ctx, consumerChannel, group, clientId, new HashSet<>()));
        assertNotNull(consumerRemotingChannel);

        assertNotSame(producerRemotingChannel, consumerRemotingChannel);
    }

    @Test
    public void testRemoveProducerChannel() {
        String group = "group";
        String clientId = RandomStringUtils.randomAlphabetic(10);

        {
            Channel producerChannel = createMockChannel();
            RemotingChannel producerRemotingChannel = this.remotingChannelManager.createProducerChannel(ctx, producerChannel, group, clientId);
            assertSame(producerRemotingChannel, this.remotingChannelManager.removeProducerChannel(ctx, group, producerRemotingChannel));
            assertTrue(this.remotingChannelManager.groupChannelMap.isEmpty());
        }
        {
            Channel producerChannel = createMockChannel();
            RemotingChannel producerRemotingChannel = this.remotingChannelManager.createProducerChannel(ctx, producerChannel, group, clientId);
            assertSame(producerRemotingChannel, this.remotingChannelManager.removeProducerChannel(ctx, group, producerChannel));
            assertTrue(this.remotingChannelManager.groupChannelMap.isEmpty());
        }
    }

    @Test
    public void testRemoveConsumerChannel() {
        String group = "group";
        String clientId = RandomStringUtils.randomAlphabetic(10);

        {
            Channel consumerChannel = createMockChannel();
            RemotingChannel consumerRemotingChannel = this.remotingChannelManager.createConsumerChannel(ctx, consumerChannel, group, clientId, new HashSet<>());
            assertSame(consumerRemotingChannel, this.remotingChannelManager.removeConsumerChannel(ctx, group, consumerRemotingChannel));
            assertTrue(this.remotingChannelManager.groupChannelMap.isEmpty());
        }
        {
            Channel consumerChannel = createMockChannel();
            RemotingChannel consumerRemotingChannel = this.remotingChannelManager.createConsumerChannel(ctx, consumerChannel, group, clientId, new HashSet<>());
            assertSame(consumerRemotingChannel, this.remotingChannelManager.removeConsumerChannel(ctx, group, consumerChannel));
            assertTrue(this.remotingChannelManager.groupChannelMap.isEmpty());
        }
    }

    @Test
    public void testRemoveChannel() {
        String consumerGroup = "consumerGroup";
        String producerGroup = "producerGroup";
        String clientId = RandomStringUtils.randomAlphabetic(10);

        Channel consumerChannel = createMockChannel();
        RemotingChannel consumerRemotingChannel = this.remotingChannelManager.createConsumerChannel(ctx, consumerChannel, consumerGroup, clientId, new HashSet<>());
        Channel producerChannel = createMockChannel();
        RemotingChannel producerRemotingChannel = this.remotingChannelManager.createProducerChannel(ctx, producerChannel, producerGroup, clientId);

        assertSame(consumerRemotingChannel, this.remotingChannelManager.removeChannel(consumerChannel).stream().findFirst().get());
        assertSame(producerRemotingChannel, this.remotingChannelManager.removeChannel(producerChannel).stream().findFirst().get());

        assertTrue(this.remotingChannelManager.groupChannelMap.isEmpty());
    }

    private Channel createMockChannel() {
        return new MockChannel(RandomStringUtils.randomAlphabetic(10));
    }

    private class MockChannel extends SimpleChannel {

        public MockChannel(String channelId) {
            super(null, new MockChannelId(channelId), RemotingChannelManagerTest.this.remoteAddress, RemotingChannelManagerTest.this.localAddress);
        }
    }

    private static class MockChannelId implements ChannelId {

        private final String channelId;

        public MockChannelId(String channelId) {
            this.channelId = channelId;
        }

        @Override
        public String asShortText() {
            return channelId;
        }

        @Override
        public String asLongText() {
            return channelId;
        }

        @Override
        public int compareTo(@NotNull ChannelId o) {
            return this.channelId.compareTo(o.asLongText());
        }
    }
}