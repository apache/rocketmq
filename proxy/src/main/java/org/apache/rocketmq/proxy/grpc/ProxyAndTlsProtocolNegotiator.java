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
package org.apache.rocketmq.proxy.grpc;

import io.grpc.Attributes;
import io.grpc.netty.shaded.io.grpc.netty.GrpcHttp2ConnectionHandler;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiators;
import io.grpc.netty.shaded.io.grpc.netty.ProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufUtil;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.handler.codec.ByteToMessageDecoder;
import io.grpc.netty.shaded.io.netty.handler.codec.ProtocolDetectionResult;
import io.grpc.netty.shaded.io.netty.handler.codec.ProtocolDetectionState;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyMessage;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyTLV;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandler;
import io.grpc.netty.shaded.io.netty.handler.ssl.SniHandler;
import io.grpc.netty.shaded.io.netty.util.AsyncMapping;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import io.grpc.netty.shaded.io.netty.util.CharsetUtil;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.BinaryUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.constant.AttributeKeys;
import org.apache.rocketmq.proxy.service.cert.TlsSniManager;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

public class ProxyAndTlsProtocolNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final String HA_PROXY_DECODER = "HAProxyDecoder";
    private static final String HA_PROXY_HANDLER = "HAProxyHandler";
    private static final String TLS_MODE_HANDLER = "TlsModeHandler";
    private static final String SNI_HANDLER = "SniHandler";
    /**
     * the length of the ssl record header (in bytes)
     */
    private static final int SSL_RECORD_HEADER_LENGTH = 5;

    private static volatile TlsSniManager tlsSniManager;

    public ProxyAndTlsProtocolNegotiator() {
        try {
            loadAllSslContexts();
            log.info("SslContext created for proxy server with SNI support");
        } catch (Exception e) {
            log.error("SslContext init error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public AsciiString scheme() {
        return AsciiString.of("https");
    }

    @Override
    public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return new ProxyAndTlsProtocolHandler(grpcHandler);
    }

    @Override
    public void close() {
    }

    private static TlsSniManager getTlsSniManager() {
        if (tlsSniManager == null) {
            synchronized (ProxyAndTlsProtocolNegotiator.class) {
                if (tlsSniManager == null) {
                    tlsSniManager = new TlsSniManager();
                    tlsSniManager.initialize(ConfigurationManager.getProxyConfig());
                }
            }
        }
        return tlsSniManager;
    }

    public static void loadAllSslContexts() {
        TlsSniManager manager = getTlsSniManager();
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        if (proxyConfig.getTlsDomainConfigs() != null && !proxyConfig.getTlsDomainConfigs().isEmpty()) {
            manager.reloadDefaultContext();
            for (String domain : proxyConfig.getTlsDomainConfigs().keySet()) {
                manager.reloadDomainContext(domain);
            }
        } else {
            manager.reloadDefaultContext();
        }
    }

    public static TlsSniManager getManager() {
        return getTlsSniManager();
    }

    private class ProxyAndTlsProtocolHandler extends ByteToMessageDecoder {

        private final GrpcHttp2ConnectionHandler grpcHandler;

        private ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.getDefault();

        public ProxyAndTlsProtocolHandler(GrpcHttp2ConnectionHandler grpcHandler) {
            this.grpcHandler = grpcHandler;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            try {
                ProtocolDetectionResult<HAProxyProtocolVersion> ha = HAProxyMessageDecoder.detectProtocol(in);
                if (ha.state() == ProtocolDetectionState.NEEDS_MORE_DATA) {
                    return;
                }
                if (ha.state() == ProtocolDetectionState.DETECTED) {
                    ctx.pipeline().addAfter(ctx.name(), HA_PROXY_DECODER, new HAProxyMessageDecoder())
                        .addAfter(HA_PROXY_DECODER, HA_PROXY_HANDLER, new HAProxyMessageHandler())
                        .addAfter(HA_PROXY_HANDLER, TLS_MODE_HANDLER, new TlsModeHandler(grpcHandler));
                } else {
                    ctx.pipeline().addAfter(ctx.name(), TLS_MODE_HANDLER, new TlsModeHandler(grpcHandler));
                }

                Attributes.Builder builder = InternalProtocolNegotiationEvent.getAttributes(pne).toBuilder();
                builder.set(AttributeKeys.CHANNEL_ID, ctx.channel().id().asLongText());

                ctx.fireUserEventTriggered(InternalProtocolNegotiationEvent.withAttributes(pne, builder.build()));
                ctx.pipeline().remove(this);
            } catch (Exception e) {
                log.error("process proxy protocol negotiator failed.", e);
                throw e;
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                pne = (ProtocolNegotiationEvent) evt;
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    private class HAProxyMessageHandler extends ChannelInboundHandlerAdapter {

        private ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.getDefault();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HAProxyMessage) {
                handleWithMessage((HAProxyMessage) msg);
                ctx.fireUserEventTriggered(pne);
            } else {
                super.channelRead(ctx, msg);
            }
            ctx.pipeline().remove(this);
        }

        private void handleWithMessage(HAProxyMessage msg) {
            try {
                Attributes.Builder builder = InternalProtocolNegotiationEvent.getAttributes(pne).toBuilder();
                if (StringUtils.isNotBlank(msg.sourceAddress())) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_ADDR, msg.sourceAddress());
                }
                if (msg.sourcePort() > 0) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_PORT, String.valueOf(msg.sourcePort()));
                }
                if (StringUtils.isNotBlank(msg.destinationAddress())) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR, msg.destinationAddress());
                }
                if (msg.destinationPort() > 0) {
                    builder.set(AttributeKeys.PROXY_PROTOCOL_SERVER_PORT, String.valueOf(msg.destinationPort()));
                }
                if (CollectionUtils.isNotEmpty(msg.tlvs())) {
                    msg.tlvs().forEach(tlv -> handleHAProxyTLV(tlv, builder));
                }
                pne = InternalProtocolNegotiationEvent
                    .withAttributes(InternalProtocolNegotiationEvent.getDefault(), builder.build());
            } finally {
                msg.release();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                pne = (ProtocolNegotiationEvent) evt;
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    protected void handleHAProxyTLV(HAProxyTLV tlv, Attributes.Builder builder) {
        byte[] valueBytes = ByteBufUtil.getBytes(tlv.content());
        if (!BinaryUtil.isAscii(valueBytes)) {
            return;
        }
        Attributes.Key<String> key = AttributeKeys.valueOf(
            HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + String.format("%02x", tlv.typeByteValue()));
        builder.set(key, new String(valueBytes, CharsetUtil.UTF_8));
    }

    private class TlsModeHandler extends ByteToMessageDecoder {

        private ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.getDefault();

        private final GrpcHttp2ConnectionHandler grpcHandler;

        public TlsModeHandler(GrpcHttp2ConnectionHandler grpcHandler) {
            this.grpcHandler = grpcHandler;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            try {
                TlsMode tlsMode = TlsSystemConfig.tlsMode;
                if (TlsMode.ENFORCING.equals(tlsMode)) {
                    addSniHandler(ctx);
                } else if (TlsMode.DISABLED.equals(tlsMode)) {
                    addPlaintextHandler(ctx);
                } else {
                    if (in.readableBytes() < SSL_RECORD_HEADER_LENGTH) {
                        return;
                    }
                    if (SslHandler.isEncrypted(in)) {
                        addSniHandler(ctx);
                    } else {
                        addPlaintextHandler(ctx);
                    }
                }
                ctx.fireUserEventTriggered(pne);
                ctx.pipeline().remove(this);
            } catch (Exception e) {
                log.error("process ssl protocol negotiator failed.", e);
                throw e;
            }
        }

        private void addSniHandler(ChannelHandlerContext ctx) {
            TlsSniManager sniManager = getTlsSniManager();
            AsyncMapping<String, SslContext> sslContextMapping = (hostname, promise) -> {
                SslContext sslCtx = sniManager.getSslContext(hostname);
                if (sslCtx != null) {
                    promise.setSuccess(sslCtx);
                } else {
                    promise.setSuccess(sniManager.getDefaultContext());
                }
                return promise;
            };
            ctx.pipeline().addAfter(ctx.name(), SNI_HANDLER, new SniHandler(sslContextMapping));
        }

        private void addPlaintextHandler(ChannelHandlerContext ctx) {
            ChannelHandler plaintext = InternalProtocolNegotiators.serverPlaintext()
                .newHandler(grpcHandler);
            ctx.pipeline().addAfter(ctx.name(), null, plaintext);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                pne = (ProtocolNegotiationEvent) evt;
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }
}
