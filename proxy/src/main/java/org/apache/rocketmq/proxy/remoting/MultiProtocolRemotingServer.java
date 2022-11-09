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

import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.cert.CertificateException;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.remoting.protocol.ProtocolNegotiationHandler;
import org.apache.rocketmq.proxy.remoting.protocol.http2proxy.Http2ProtocolProxyHandler;
import org.apache.rocketmq.proxy.remoting.protocol.remoting.RemotingProtocolHandler;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * for remoting server, if config listen port is 8080 in nettyServerConfig
 * <p>
 * will
 * <li>listen port at 9080 with protocol remoting</li>
 * <li>listen port at 8080 with protocol remoting and http2</li>
 */
public class MultiProtocolRemotingServer extends NettyRemotingServer {

    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final int PORT_DELTA = 1000;
    private final NettyServerConfig nettyServerConfig;
    private final int port;

    public MultiProtocolRemotingServer(NettyServerConfig nettyServerConfig, ChannelEventListener channelEventListener) {
        super(nettyServerConfig, channelEventListener);
        this.port = nettyServerConfig.getListenPort();
        // to support multiple protocol
        // will bind the real port in configChildHandler
        // so let parent bind to a useless port
        nettyServerConfig.setListenPort(nettyServerConfig.getListenPort() + PORT_DELTA);
        this.nettyServerConfig = nettyServerConfig;
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
    public void start() {
        super.start();
        this.configChildHandler();
    }

    protected void configChildHandler() {
        try {
            ServerBootstrap serverBootstrap = getField("serverBootstrap", ServerBootstrap.class);
            Preconditions.checkNotNull(serverBootstrap);
            DefaultEventExecutorGroup defaultEventExecutorGroup = getField("defaultEventExecutorGroup", DefaultEventExecutorGroup.class);
            Preconditions.checkNotNull(defaultEventExecutorGroup);
            NettyEncoder encoder = getField("encoder", NettyEncoder.class);
            Preconditions.checkNotNull(encoder);
            ChannelDuplexHandler connectionManageHandler = getField("connectionManageHandler", ChannelDuplexHandler.class);
            Preconditions.checkNotNull(connectionManageHandler);
            SimpleChannelInboundHandler serverHandler = getField("serverHandler", SimpleChannelInboundHandler.class);
            Preconditions.checkNotNull(serverHandler);
            SimpleChannelInboundHandler handshakeHandler = getField("handshakeHandler", SimpleChannelInboundHandler.class);
            Preconditions.checkNotNull(handshakeHandler);
            ConcurrentMap remotingServerTable = getField("remotingServerTable", ConcurrentMap.class);
            Preconditions.checkNotNull(remotingServerTable);

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(defaultEventExecutorGroup, "handshakeHandler", handshakeHandler)
                        .addLast(defaultEventExecutorGroup,
                            new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                            new ProtocolNegotiationHandler(new RemotingProtocolHandler(encoder, connectionManageHandler, serverHandler))
                                .addProtocolHandler(new Http2ProtocolProxyHandler())
                        );
                }
            });
            remotingServerTable.put(port, this);
            serverBootstrap.bind(port).sync();
        } catch (Throwable t) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "config netty child handler failed", t);
        }
    }

    protected <T> T getField(String name, Class<T> getClazz) throws Throwable {
        Field field = NettyRemotingServer.class.getDeclaredField(name);
        field.setAccessible(true);
        return getClazz.cast(field.get(this));
    }
}
