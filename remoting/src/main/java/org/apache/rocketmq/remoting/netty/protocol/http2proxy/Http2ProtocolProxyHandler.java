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

package org.apache.rocketmq.remoting.netty.protocol.http2proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.protocol.ProtocolHandler;

public class Http2ProtocolProxyHandler implements ProtocolHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * The int value of "PRI ". Now use 4 bytes to judge protocol, may be has potential risks if there is a new protocol
     * which start with "PRI " too in the future
     * <p>
     * The full HTTP/2 connection preface is "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
     * <p>
     * ref: https://datatracker.ietf.org/doc/html/rfc7540#section-3.5
     */
    private static final int PRI_INT = 0x50524920;

    private final NettyServerConfig nettyServerConfig;
    private final SslContext sslContext;

    public Http2ProtocolProxyHandler(NettyServerConfig nettyServerConfig) {
        if (nettyServerConfig.isEnableHttp2SslProxy()) {
            try {
                sslContext = SslContextBuilder
                    .forClient()
                    .sslProvider(SslProvider.OPENSSL)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                        ApplicationProtocolConfig.Protocol.ALPN,
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.HTTP_2))
                    .build();
            } catch (SSLException e) {
                log.error("Failed to create SSLContext for Http2ProtocolProxyHandler", e);
                throw new RuntimeException("Failed to create SSLContext for Http2ProtocolProxyHandler", e);
            }
        } else {
            sslContext = null;
        }
        this.nettyServerConfig = nettyServerConfig;
    }

    @Override
    public boolean match(ByteBuf in) {
        if (!nettyServerConfig.isEnableHttp2Proxy()) {
            return false;
        }

        // If starts with 'PRI '
        return in.getInt(in.readerIndex()) == PRI_INT;
    }

    @Override
    public void config(final ChannelHandlerContext ctx, final ByteBuf msg) {
        // proxy channel to http2 server
        final Channel inboundChannel = ctx.channel();

        // Start the connection attempt.
        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop())
            .channel(ctx.channel().getClass())
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    if (sslContext != null && nettyServerConfig.isEnableHttp2SslProxy()) {
                        ch.pipeline()
                            .addLast(sslContext.newHandler(ch.alloc(), nettyServerConfig.getHttp2ProxyHost(), nettyServerConfig.getHttp2ProxyPort()));
                    }
                    ch.pipeline().addLast(new Http2ProxyBackendHandler(inboundChannel));
                }
            })
            .option(ChannelOption.AUTO_READ, false);
        ChannelFuture f = b.connect(nettyServerConfig.getHttp2ProxyHost(), nettyServerConfig.getHttp2ProxyPort());
        final Channel outboundChannel = f.channel();

        ctx.pipeline().addLast(new Http2ProxyFrontendHandler(outboundChannel));

        msg.retain();
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    outboundChannel.writeAndFlush(msg);
                    inboundChannel.read();
                } else {
                    inboundChannel.close();
                }
            }
        });
    }
}
