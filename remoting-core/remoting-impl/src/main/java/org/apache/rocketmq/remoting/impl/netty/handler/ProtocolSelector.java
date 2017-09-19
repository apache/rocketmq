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

package org.apache.rocketmq.remoting.impl.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.remoting.api.channel.ChannelHandlerContextWrapper;
import org.apache.rocketmq.remoting.api.protocol.Protocol;
import org.apache.rocketmq.remoting.api.protocol.ProtocolFactory;
import org.apache.rocketmq.remoting.impl.channel.ChannelHandlerContextWrapperImpl;
import org.apache.rocketmq.remoting.impl.protocol.ProtocolFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtocolSelector extends SimpleChannelInboundHandler<ByteBuf> {
    public static final String NAME = ProtocolSelector.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolSelector.class);
    private ProtocolFactory protocolFactory;

    public ProtocolSelector(final SslContext sslContext) {
        this.protocolFactory = new ProtocolFactoryImpl(sslContext);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        if (msg.readableBytes() < 1) {
            return;
        }
        msg.markReaderIndex();
        Protocol protocol = protocolFactory.get(msg.readByte());
        if (protocol == null) {
            ctx.channel().close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOG.warn("Close channel {},result is {}", ctx.channel(), future.isSuccess());
                }
            });
            return;
        }
        ChannelHandlerContextWrapper chcw = new ChannelHandlerContextWrapperImpl(ctx);
        protocol.assembleHandler(chcw);
        msg.resetReaderIndex();
        ctx.pipeline().remove(this);
        ctx.fireChannelRead(msg.retain());
    }
}
