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

package org.apache.rocketmq.proxy.grpc.v2.channel;

import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GrpcClientChannelTest extends InitConfigTest {

    @Mock
    private ProxyRelayService proxyRelayService;
    @Mock
    private GrpcClientSettingsManager grpcClientSettingsManager;
    @Mock
    private GrpcChannelManager grpcChannelManager;

    private String clientId;
    private GrpcClientChannel grpcClientChannel;

    @Before
    public void before() throws Throwable {
        super.before();
        this.clientId = RandomStringUtils.randomAlphabetic(10);
        this.grpcClientChannel = new GrpcClientChannel(proxyRelayService, grpcClientSettingsManager, grpcChannelManager,
            ProxyContext.create().setRemoteAddress("10.152.39.53:9768").setLocalAddress("11.193.0.1:1210"),
            this.clientId);
    }

    @Test
    public void testChannelExtendAttributeParse() {
        Settings clientSettings = Settings.newBuilder()
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder()
                    .setName("topic")
                    .build())
                .build())
            .build();
        when(grpcClientSettingsManager.getRawClientSettings(eq(clientId))).thenReturn(clientSettings);

        RemoteChannel remoteChannel = this.grpcClientChannel.toRemoteChannel();
        assertEquals(ChannelProtocolType.GRPC_V2, remoteChannel.getType());
        assertEquals(clientSettings, GrpcClientChannel.parseChannelExtendAttribute(remoteChannel));
        assertEquals(clientSettings, GrpcClientChannel.parseChannelExtendAttribute(this.grpcClientChannel));
        assertNull(GrpcClientChannel.parseChannelExtendAttribute(mock(RemotingChannel.class)));
    }
}