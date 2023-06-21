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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AbstractRemotingActivityTest extends InitConfigTest {

    private static final String CLIENT_ID = "test@clientId";
    AbstractRemotingActivity remotingActivity;
    @Mock
    MessagingProcessor messagingProcessorMock;
    @Spy
    ChannelHandlerContext ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "0.0.0.0:0", "1.1.1.1:1")) {
        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }
    };

    @Before
    public void setup() {
        remotingActivity = new AbstractRemotingActivity(null, messagingProcessorMock) {
            @Override
            protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
                ProxyContext context) throws Exception {
                return null;
            }
        };
        Channel channel = ctx.channel();
        RemotingHelper.setPropertyToAttr(channel, RemotingHelper.CLIENT_ID_KEY, CLIENT_ID);
        RemotingHelper.setPropertyToAttr(channel, RemotingHelper.LANGUAGE_CODE_KEY, LanguageCode.JAVA);
        RemotingHelper.setPropertyToAttr(channel, RemotingHelper.VERSION_KEY, MQVersion.CURRENT_VERSION);
    }

    @Test
    public void testCreateContext() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        ProxyContext context = remotingActivity.createContext(ctx, request);

        Assert.assertEquals(context.getAction(), RemotingHelper.getRequestCodeDesc(RequestCode.PULL_MESSAGE));
        Assert.assertEquals(context.getProtocolType(), ChannelProtocolType.REMOTING.getName());
        Assert.assertEquals(context.getLanguage(), LanguageCode.JAVA.name());
        Assert.assertEquals(context.getClientID(), CLIENT_ID);
        Assert.assertEquals(context.getClientVersion(), MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

    }

    @Test
    public void testRequest() throws Exception {
        String brokerName = "broker";
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "remark");
        when(messagingProcessorMock.request(any(), eq(brokerName), any(), anyLong())).thenReturn(CompletableFuture.completedFuture(
            response
        ));
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(ctx, times(1)).writeAndFlush(response);
    }

    @Test
    public void testRequestOneway() throws Exception {
        String brokerName = "broker";
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.markOnewayRPC();
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(messagingProcessorMock, times(1)).requestOneway(any(), eq(brokerName), any(), anyLong());
    }

    @Test
    public void testRequestInvalid() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField("test", "test");
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.VERSION_NOT_SUPPORTED);
        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    public void testRequestProxyException() throws Exception {
        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        String brokerName = "broker";
        String remark = "exception";
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new ProxyException(ProxyExceptionCode.FORBIDDEN, remark));
        when(messagingProcessorMock.request(any(), eq(brokerName), any(), anyLong())).thenReturn(future);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(ctx, times(1)).writeAndFlush(captor.capture());
        assertThat(captor.getValue().getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testRequestClientException() throws Exception {
        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        String brokerName = "broker";
        String remark = "exception";
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new MQClientException(remark, null));
        when(messagingProcessorMock.request(any(), eq(brokerName), any(), anyLong())).thenReturn(future);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(ctx, times(1)).writeAndFlush(captor.capture());
        assertThat(captor.getValue().getCode()).isEqualTo(-1);
    }

    @Test
    public void testRequestBrokerException() throws Exception {
        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        String brokerName = "broker";
        String remark = "exception";
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new MQBrokerException(ResponseCode.FLUSH_DISK_TIMEOUT, remark));
        when(messagingProcessorMock.request(any(), eq(brokerName), any(), anyLong())).thenReturn(future);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(ctx, times(1)).writeAndFlush(captor.capture());
        assertThat(captor.getValue().getCode()).isEqualTo(ResponseCode.FLUSH_DISK_TIMEOUT);
    }

    @Test
    public void testRequestAclException() throws Exception {
        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        String brokerName = "broker";
        String remark = "exception";
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new AclException(remark, ResponseCode.MESSAGE_ILLEGAL));
        when(messagingProcessorMock.request(any(), eq(brokerName), any(), anyLong())).thenReturn(future);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(ctx, times(1)).writeAndFlush(captor.capture());
        assertThat(captor.getValue().getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testRequestDefaultException() throws Exception {
        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        String brokerName = "broker";
        String remark = "exception";
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception(remark));
        when(messagingProcessorMock.request(any(), eq(brokerName), any(), anyLong())).thenReturn(future);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(ctx, times(1)).writeAndFlush(captor.capture());
        assertThat(captor.getValue().getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }
}