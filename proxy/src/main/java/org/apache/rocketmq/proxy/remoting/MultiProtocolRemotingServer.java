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

package org.apache.rocketmq.proxy.remoting;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.remoting.protocol.ProtocolNegotiationHandler;
import org.apache.rocketmq.proxy.remoting.protocol.http2proxy.Http2ProtocolProxyHandler;
import org.apache.rocketmq.proxy.remoting.protocol.remoting.RemotingProtocolHandler;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

import java.io.IOException;
import java.security.cert.CertificateException;

/**
 * support remoting and http2 protocol at one port
 */
public class MultiProtocolRemotingServer extends NettyRemotingServer {

    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final NettyServerConfig nettyServerConfig;

    private final RemotingProtocolHandler remotingProtocolHandler;
    protected Http2ProtocolProxyHandler http2ProtocolProxyHandler;

    public MultiProtocolRemotingServer(NettyServerConfig nettyServerConfig, ChannelEventListener channelEventListener) {
        super(nettyServerConfig, channelEventListener);
        this.nettyServerConfig = nettyServerConfig;

        this.remotingProtocolHandler = new RemotingProtocolHandler(
            this::getEncoder,
            this::getDistributionHandler,
            this::getConnectionManageHandler,
            this::getServerHandler);
        this.http2ProtocolProxyHandler = new Http2ProtocolProxyHandler();
    }

    @Override
    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = MultiProtocolTlsHelper.buildSslContext();
                log.info("SSLContext created for server");
            } catch (CertificateException | IOException e) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "Failed to create SSLContext for server", e);
            }
        }
    }

    @Override
    protected ChannelPipeline configChannel(SocketChannel ch) {
        return ch.pipeline()
            .addLast(HANDSHAKE_HANDLER_NAME, new HandshakeHandler())
            .addLast(
                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                new ProtocolNegotiationHandler(this.remotingProtocolHandler)
                    .addProtocolHandler(this.http2ProtocolProxyHandler)
            );
    }
}
