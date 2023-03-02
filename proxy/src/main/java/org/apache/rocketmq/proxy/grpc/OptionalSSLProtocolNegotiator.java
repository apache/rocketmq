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

import io.grpc.netty.shaded.io.grpc.netty.GrpcHttp2ConnectionHandler;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiators;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.handler.codec.ByteToMessageDecoder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandler;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class OptionalSSLProtocolNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final SslContext sslContext;
    /**
     * the length of the ssl record header (in bytes)
     */
    private static final int SSL_RECORD_HEADER_LENGTH = 5;

    public OptionalSSLProtocolNegotiator(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public AsciiString scheme() {
        return AsciiString.of("https");
    }

    @Override
    public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHttp2ConnectionHandler) {
        ChannelHandler plaintext =
            InternalProtocolNegotiators.serverPlaintext().newHandler(grpcHttp2ConnectionHandler);
        ChannelHandler ssl =
            InternalProtocolNegotiators.serverTls(sslContext).newHandler(grpcHttp2ConnectionHandler);
        return new PortUnificationServerHandler(ssl, plaintext);
    }

    @Override
    public void close() {}

    public static class PortUnificationServerHandler extends ByteToMessageDecoder {
        private final ChannelHandler ssl;
        private final ChannelHandler plaintext;

        public PortUnificationServerHandler(ChannelHandler ssl, ChannelHandler plaintext) {
            this.ssl = ssl;
            this.plaintext = plaintext;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
            try {
                // in SslHandler.isEncrypted, it need at least 5 bytes to judge is encrypted or not
                if (in.readableBytes() < SSL_RECORD_HEADER_LENGTH) {
                    return;
                }
                if (SslHandler.isEncrypted(in)) {
                    ctx.pipeline().addAfter(ctx.name(), null, this.ssl);
                } else {
                    ctx.pipeline().addAfter(ctx.name(), null, this.plaintext);
                }
                ctx.fireUserEventTriggered(InternalProtocolNegotiationEvent.getDefault());
                ctx.pipeline().remove(this);
            } catch (Exception e) {
                log.error("process protocol negotiator failed.", e);
                throw e;
            }
        }
    }
}