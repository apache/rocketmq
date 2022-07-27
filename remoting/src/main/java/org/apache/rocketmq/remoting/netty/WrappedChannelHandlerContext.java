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

package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.net.SocketAddress;

/**
 * <p>Netty's ChannelHandlerContext is way too powerful and is easy to be incorrectly used, such as calling writeAndFlush
 * in the Processor resulting the break of hook lifecycle.</p>
 *
 * <p>Please use WrappedChannelHandlerContext in NettyRequestProcessor instead of ChannelHandlerContext.</p>
 */
public class WrappedChannelHandlerContext {
    // Please DO NOT USE ctx in processor directly!
    private final ChannelHandlerContext ctx;

    public WrappedChannelHandlerContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public String channelInfo() {
        return this.ctx.channel().toString();
    }

    public SocketAddress remoteAddress() {
        return this.ctx.channel().remoteAddress();
    }

    public String channelRemoteAddr() {
        return RemotingHelper.parseChannelRemoteAddr(this.ctx.channel());
    }

    public boolean channelIsWritable(boolean isNetWorkFlowController) {
        if (isNetWorkFlowController) {
            return this.ctx.channel().isWritable();
        }
        return true;
    }

    public boolean isChannelActive() {
        return this.ctx.channel().isActive();
    }

    // Please TRY NOT USE this method in processor directly.
    public void response(RemotingCommand response, ChannelFutureListener channelFutureListener) {
        this.ctx.channel().writeAndFlush(response).addListener(channelFutureListener);
    }

    public WrappedChannel getWrappedChannel() {
        return new WrappedChannel(this.ctx.channel());
    }
}
