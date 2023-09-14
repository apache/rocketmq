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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.remoting.protocol.ProtocolHandler;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

import javax.net.ssl.SSLException;

public class Http2ProtocolProxyHandler implements ProtocolHandler {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);
    private static final String LOCAL_HOST = "127.0.0.1";
    /**
     * The int value of "PRI ". Now use 4 bytes to judge protocol, may be has potential risks if there is a new protocol
     * which start with "PRI " in the future
     * <p>
     * The full HTTP/2 connection preface is "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
     * <p>
     * ref: https://datatracker.ietf.org/doc/html/rfc7540#section-3.5
     */
    private static final int PRI_INT = 0x50524920;

    private final SslContext sslContext;

    public Http2ProtocolProxyHandler() {
        try {
            TlsMode tlsMode = TlsSystemConfig.tlsMode;
            if (TlsMode.DISABLED.equals(tlsMode)) {
                sslContext = null;
            } else {
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
            }
        } catch (SSLException e) {
            log.error("Failed to create SSLContext for Http2ProtocolProxyHandler", e);
            throw new RuntimeException("Failed to create SSLContext for Http2ProtocolProxyHandler", e);
        }
    }

    @Override
    public boolean match(ByteBuf in) {
        if (!ConfigurationManager.getProxyConfig().isEnableRemotingLocalProxyGrpc()) {
            return false;
        }

        // If starts with 'PRI '
        return in.getInt(in.readerIndex()) == PRI_INT;
    }

    @Override
    public void config(final ChannelHandlerContext ctx, final ByteBuf msg) {
        // proxy channel to http2 server
        final Channel inboundChannel = ctx.channel();

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        // Start the connection attempt.
        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop())
            .channel(ctx.channel().getClass())
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline().addLast(null, Http2ProxyBackendHandler.HANDLER_NAME,
                            new Http2ProxyBackendHandler(inboundChannel));
                }
            })
            .option(ChannelOption.AUTO_READ, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getLocalProxyConnectTimeoutMs());
        ChannelFuture f;
        try {
            f = b.connect(LOCAL_HOST, config.getGrpcServerPort()).sync();
        } catch (Exception e) {
            log.error("connect http2 server failed. port:{}", config.getGrpcServerPort(), e);
            inboundChannel.close();
            return;
        }

        final Channel outboundChannel = f.channel();
        if (inboundChannel.hasAttr(AttributeKeys.PROXY_PROTOCOL_ADDR)) {
            ctx.pipeline().addLast(new HAProxyMessageForwarder(outboundChannel));
            outboundChannel.pipeline().addFirst(HAProxyMessageEncoder.INSTANCE);
        }

        SslHandler sslHandler = null;
        if (sslContext != null) {
            sslHandler = sslContext.newHandler(outboundChannel.alloc(), LOCAL_HOST, config.getGrpcServerPort());
        }
        ctx.pipeline().addLast(new Http2ProxyFrontendHandler(outboundChannel, sslHandler));
    }
}
