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
import io.grpc.InternalChannelz;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
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
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandler;
import io.grpc.netty.shaded.io.netty.handler.ssl.SniHandler;
import io.grpc.netty.shaded.io.netty.util.AsyncMapping;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import io.grpc.netty.shaded.io.netty.util.CharsetUtil;

import javax.net.ssl.SSLSession;

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
    private static final String GRPC_SNI_COMPLETE_HANDLER = "GrpcSniCompleteHandler";
    /**
     * the length of the ssl record header (in bytes)
     */
    private static final int SSL_RECORD_HEADER_LENGTH = 5;

    public ProxyAndTlsProtocolNegotiator() {
        try {
            // Ensure TlsSniManager is initialized with all configured domain contexts.
            // No need to call loadAllSslContexts() here — getInstance() triggers initialize().
            getTlsSniManager();
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
        return TlsSniManager.getInstance();
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
                try {
                    SslContext sslCtx = sniManager.getSslContext(hostname != null ? hostname.toLowerCase(java.util.Locale.ROOT) : null);
                    if (sslCtx == null) {
                        sslCtx = sniManager.getDefaultContext();
                    }
                    if (sslCtx == null) {
                        promise.setFailure(new javax.net.ssl.SSLException("No SslContext available for SNI hostname: " + hostname));
                    } else {
                        promise.setSuccess(sslCtx);
                    }
                } catch (Exception e) {
                    promise.setFailure(e);
                }
                return promise;
            };
            // Pipeline order after this call:
            //   TlsModeHandler -> SniHandler (becomes SslHandler after SNI) -> GrpcSniCompleteHandler -> ...
            // GrpcSniCompleteHandler is AFTER SniHandler so it receives SslHandshakeCompletionEvent (inbound, head→tail)
            ctx.pipeline()
                .addAfter(ctx.name(), SNI_HANDLER, new SniHandler(sslContextMapping))
                .addAfter(SNI_HANDLER, GRPC_SNI_COMPLETE_HANDLER, new GrpcSniHandshakeCompleteHandler(grpcHandler, pne));
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

    /**
     * Placed AFTER the SniHandler/SslHandler in the pipeline.
     * Receives SslHandshakeCompletionEvent (inbound, fires head→tail), then:
     *  - verifies ALPN negotiated "h2" (warns and continues if no ALPN, to support test mode)
     *  - builds enriched Attributes + InternalChannelz.Security from the SSL session
     *  - replaces itself with grpcHandler
     *  - calls grpcHandler.handleProtocolNegotiationCompleted to wire up gRPC HTTP/2
     */
    private static class GrpcSniHandshakeCompleteHandler extends ChannelInboundHandlerAdapter {

        private final GrpcHttp2ConnectionHandler grpcHandler;
        private ProtocolNegotiationEvent pne;

        GrpcSniHandshakeCompleteHandler(GrpcHttp2ConnectionHandler grpcHandler, ProtocolNegotiationEvent pne) {
            this.grpcHandler = grpcHandler;
            this.pne = pne;
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof ProtocolNegotiationEvent) {
                // Accumulate upstream attributes (e.g. HA proxy)
                pne = (ProtocolNegotiationEvent) evt;
                return;
            }
            if (evt instanceof SslHandshakeCompletionEvent) {
                SslHandshakeCompletionEvent event = (SslHandshakeCompletionEvent) evt;
                try {
                    if (!event.isSuccess()) {
                        log.warn("SNI TLS handshake failed", event.cause());
                        ctx.fireExceptionCaught(event.cause());
                        ctx.pipeline().remove(this);
                        return;
                    }
                    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
                    if (sslHandler == null) {
                        Exception ex = new javax.net.ssl.SSLException("SslHandler not found in pipeline after SNI handshake");
                        ctx.fireExceptionCaught(ex);
                        ctx.pipeline().remove(this);
                        return;
                    }
                    // ALPN check: gRPC requires "h2"; in test/permissive mode ALPN may be absent
                    String protocol = sslHandler.applicationProtocol();
                    if (protocol != null && !protocol.isEmpty() && !"h2".equals(protocol)) {
                        Exception ex = new javax.net.ssl.SSLException(
                            "Failed protocol negotiation: expected h2 but got " + protocol);
                        ctx.fireExceptionCaught(ex);
                        ctx.pipeline().remove(this);
                        return;
                    }

                    // Build enriched Attributes and Security from SSL session
                    SSLSession sslSession = sslHandler.engine().getSession();
                    InternalChannelz.Security security = new InternalChannelz.Security(
                        new InternalChannelz.Tls(sslSession));
                    Attributes attrs = InternalProtocolNegotiationEvent.getAttributes(pne).toBuilder()
                        .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.PRIVACY_AND_INTEGRITY)
                        .set(io.grpc.Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession)
                        .build();

                    // Replace this handler with grpcHandler, then complete negotiation
                    ctx.pipeline().replace(this, null, grpcHandler);
                    grpcHandler.handleProtocolNegotiationCompleted(attrs, security);
                } catch (Exception e) {
                    log.error("Error completing SNI TLS handshake", e);
                    ctx.fireExceptionCaught(e);
                    ctx.pipeline().remove(this);
                }
                return;
            }
            super.userEventTriggered(ctx, evt);
        }
    }
}
