/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.proxy.remoting.protocol.http2proxy;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Http2ProxyChannelCleanupTest {

    @Test
    public void backendWriteFailureClosesBothChannels() throws Exception {
        Channel inboundChannel = mock(Channel.class);
        Channel backendChannel = mock(Channel.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        ChannelFuture writeFuture = failedWriteFuture(inboundChannel);
        Object message = new Object();

        when(context.channel()).thenReturn(backendChannel);
        when(inboundChannel.writeAndFlush(message)).thenReturn(writeFuture);

        new Http2ProxyBackendHandler(inboundChannel).channelRead(context, message);

        verify(inboundChannel).close();
        verify(backendChannel).close();
    }

    @Test
    public void backendExceptionClosesBothChannels() throws Exception {
        Channel inboundChannel = mock(Channel.class);
        Channel backendChannel = mock(Channel.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        stubCloseOnFlush(inboundChannel);
        stubCloseOnFlush(backendChannel);
        when(context.channel()).thenReturn(backendChannel);

        new Http2ProxyBackendHandler(inboundChannel).exceptionCaught(context, new RuntimeException("test"));

        verify(inboundChannel).writeAndFlush(Unpooled.EMPTY_BUFFER);
        verify(backendChannel).writeAndFlush(Unpooled.EMPTY_BUFFER);
    }

    @Test
    public void frontendWriteFailureClosesBothChannels() throws Exception {
        Channel frontendChannel = mock(Channel.class);
        Channel outboundChannel = mock(Channel.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        ChannelFuture writeFuture = failedWriteFuture(outboundChannel);
        Object message = new Object();

        when(context.channel()).thenReturn(frontendChannel);
        when(outboundChannel.isActive()).thenReturn(true);
        when(outboundChannel.writeAndFlush(message)).thenReturn(writeFuture);

        new Http2ProxyFrontendHandler(outboundChannel, null).channelRead(context, message);

        verify(outboundChannel).close();
        verify(frontendChannel).close();
    }

    @Test
    public void frontendExceptionClosesBothChannels() throws Exception {
        Channel frontendChannel = mock(Channel.class);
        Channel outboundChannel = mock(Channel.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        stubCloseOnFlush(frontendChannel);
        stubCloseOnFlush(outboundChannel);
        when(context.channel()).thenReturn(frontendChannel);

        new Http2ProxyFrontendHandler(outboundChannel, null)
            .exceptionCaught(context, new RuntimeException("test"));

        verify(frontendChannel).writeAndFlush(Unpooled.EMPTY_BUFFER);
        verify(outboundChannel).writeAndFlush(Unpooled.EMPTY_BUFFER);
    }

    private static ChannelFuture failedWriteFuture(Channel channel) {
        ChannelFuture future = mock(ChannelFuture.class);
        when(future.channel()).thenReturn(channel);
        when(future.isSuccess()).thenReturn(false);
        doAnswer(invocation -> {
            ChannelFutureListener listener = invocation.getArgument(0);
            listener.operationComplete(future);
            return future;
        }).when(future).addListener(any(ChannelFutureListener.class));
        return future;
    }

    private static void stubCloseOnFlush(Channel channel) {
        ChannelFuture future = mock(ChannelFuture.class);
        when(channel.isActive()).thenReturn(true);
        when(channel.writeAndFlush(Unpooled.EMPTY_BUFFER)).thenReturn(future);
        when(future.addListener(ChannelFutureListener.CLOSE)).thenReturn(future);
    }
}
