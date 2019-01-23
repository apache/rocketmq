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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.net.SocketAddress;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class NettyChannelImpl implements RemotingChannel {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private final Channel channel;

    private ChannelHandlerContext channelHandlerContext;

    public NettyChannelImpl(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
        this.channel = channelHandlerContext.channel();
    }

    public NettyChannelImpl(Channel channel) {
        this.channel = channel;
    }

    @Override
    public SocketAddress localAddress() {
        return channel.localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    @Override
    public boolean isWritable() {
        return channel.isWritable();
    }

    @Override
    public boolean isActive() {
        return channel.isActive();
    }

    @Override
    public void close() {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                log.info("CloseChannel NettyChannelImpl: close the connection to remote address[{}] result: {}", addrRemote,
                    future.isSuccess());
            }
        });
    }

    @Override
    public void reply(final RemotingCommand response) {
        if (response != null) {
            response.markResponseType();
            try {
                channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            log.error("ProcessRequestWrapper response to {} failed",
                                future.channel().remoteAddress(), future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("ProcessRequestWrapper process request over, but response failed", e);
                log.error(response.toString());
            }
        }
    }

    public Channel getChannel() {
        return channel;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final NettyChannelImpl that = (NettyChannelImpl) o;

        return channel != null ? channel.equals(that.channel) : that.channel == null;
    }

    @Override
    public int hashCode() {
        return channel != null ? channel.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "NettyChannelImpl [channel=" + channel + "]";
    }
}
