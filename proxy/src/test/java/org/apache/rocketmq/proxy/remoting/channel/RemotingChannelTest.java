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
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.remoting.RemotingProxyOutClient;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RemotingChannelTest extends InitConfigTest {
    @Mock
    private RemotingProxyOutClient remotingProxyOutClient;
    @Mock
    private ProxyRelayService proxyRelayService;
    @Mock
    private Channel parent;

    private String clientId;
    private Set<SubscriptionData> subscriptionData;
    private RemotingChannel remotingChannel;

    private final String remoteAddress = "10.152.39.53:9768";
    private final String localAddress = "11.193.0.1:1210";

    @Before
    public void before() throws Throwable {
        super.before();
        this.clientId = RandomStringUtils.randomAlphabetic(10);
        when(parent.remoteAddress()).thenReturn(NetworkUtil.string2SocketAddress(remoteAddress));
        when(parent.localAddress()).thenReturn(NetworkUtil.string2SocketAddress(localAddress));
        this.subscriptionData = new HashSet<>();
        this.subscriptionData.add(FilterAPI.buildSubscriptionData("topic", "subTag"));
        this.remotingChannel = new RemotingChannel(remotingProxyOutClient, proxyRelayService,
            parent, clientId, subscriptionData);
    }

    @Test
    public void testChannelExtendAttributeParse() {
        RemoteChannel remoteChannel = this.remotingChannel.toRemoteChannel();
        assertEquals(ChannelProtocolType.REMOTING, remoteChannel.getType());
        assertEquals(subscriptionData, RemotingChannel.parseChannelExtendAttribute(remoteChannel));
        assertEquals(subscriptionData, RemotingChannel.parseChannelExtendAttribute(this.remotingChannel));
        assertNull(RemotingChannel.parseChannelExtendAttribute(mock(GrpcClientChannel.class)));
    }
}