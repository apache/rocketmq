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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AbstractRemotingActivityTest extends InitConfigAndLoggerTest {
    AbstractRemotingActivity remotingActivity;
    @Mock
    MessagingProcessor messagingProcessorMock;
    @Spy
    ChannelHandlerContext ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "1", "2")) {
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
    }

    @Test
    public void request() throws Exception {
        String brokerName = "broker";
        String remark = "success";
        when(messagingProcessorMock.request(any(), anyString(), any(), anyLong())).thenReturn(CompletableFuture.completedFuture(
            RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, remark)
        ));
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(AbstractRemotingActivity.BROKER_NAME_FIELD, brokerName);
        RemotingCommand remotingCommand = remotingActivity.request(ctx, request, null, 10000);
        assertThat(remotingCommand).isNull();
        verify(ctx, times(1)).writeAndFlush(any());
    }
}