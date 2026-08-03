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
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannel;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    @Test
    public void testGetConsumerRunningInfoShouldFailFastWhenObserverIsMissing() throws Exception {
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture = new CompletableFuture<>();
        when(grpcChannelManager.addResponseFuture(eq(responseFuture))).thenReturn("nonce-1");
        when(grpcChannelManager.getAndRemoveResponseFuture(eq("nonce-1"))).thenReturn((CompletableFuture) responseFuture);

        GetConsumerRunningInfoRequestHeader header = new GetConsumerRunningInfoRequestHeader();
        header.setJstackEnable(true);

        grpcClientChannel.processGetConsumerRunningInfo(mock(RemotingCommand.class), header, responseFuture).get();

        assertTrue(responseFuture.isDone());
        ProxyRelayResult<ConsumerRunningInfo> result = responseFuture.get();
        assertEquals(ResponseCode.SYSTEM_BUSY, result.getCode());
        assertEquals("write telemetry command failed", result.getRemark());
        verify(grpcChannelManager).getAndRemoveResponseFuture("nonce-1");
    }

    @Test
    public void testConsumeMessageDirectlyShouldFailFastWhenObserverWriteFails() throws Exception {
        StreamObserver<TelemetryCommand> observer = mock(StreamObserver.class);
        doThrow(new IllegalStateException("stream closed")).when(observer).onNext(any(TelemetryCommand.class));
        grpcClientChannel.setClientObserver(observer);

        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture = new CompletableFuture<>();
        when(grpcChannelManager.addResponseFuture(eq(responseFuture))).thenReturn("nonce-2");
        when(grpcChannelManager.getAndRemoveResponseFuture(eq("nonce-2"))).thenReturn((CompletableFuture) responseFuture);

        grpcClientChannel.processConsumeMessageDirectly(
            mock(RemotingCommand.class),
            new ConsumeMessageDirectlyResultRequestHeader(),
            buildMessageExt(),
            responseFuture
        ).get();

        assertTrue(responseFuture.isDone());
        ProxyRelayResult<ConsumeMessageDirectlyResult> result = responseFuture.get();
        assertEquals(ResponseCode.SYSTEM_BUSY, result.getCode());
        assertEquals("write telemetry command failed", result.getRemark());
        verify(grpcChannelManager).getAndRemoveResponseFuture("nonce-2");
        assertFalse(grpcClientChannel.isOpen());
    }

    private MessageExt buildMessageExt() {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic("test-topic");
        messageExt.setBody("hello".getBytes(StandardCharsets.UTF_8));
        messageExt.setMsgId("msg-id");
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setStoreTimestamp(System.currentTimeMillis());
        messageExt.putUserProperty("test", "true");
        return messageExt;
    }
}
