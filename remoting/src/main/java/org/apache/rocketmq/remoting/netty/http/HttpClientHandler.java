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
package org.apache.rocketmq.remoting.netty.http;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import com.alibaba.fastjson.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;

public class HttpClientHandler extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private SimpleChannelInboundHandler<RemotingCommand> channelInboundHandler;

    public HttpClientHandler(SimpleChannelInboundHandler<RemotingCommand> channelInboundHandler) {
        this.channelInboundHandler = channelInboundHandler;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        RemotingCommand remotingCommand = null;
        try {
            ByteBuf buf = null;
            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                buf = response.content();
            } else if (msg instanceof DefaultLastHttpContent) {
                DefaultLastHttpContent defaultLastHttpContent = (DefaultLastHttpContent) msg;
                buf = defaultLastHttpContent.content();
            }
            if (buf != null) {
                byte[] by = new byte[buf.writerIndex()];
                buf.getBytes(0, by);
                remotingCommand = JSON.parseObject(by, RemotingCommand.class);
                this.channelInboundHandler.channelRead(ctx, remotingCommand);
                ctx.channel().closeFuture();
            }
        } catch (Exception e) {
            ctx.channel().closeFuture();
            log.error("client decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
        }
    }
}
