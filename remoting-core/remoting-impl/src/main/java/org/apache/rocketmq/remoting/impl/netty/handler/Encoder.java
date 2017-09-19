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

package org.apache.rocketmq.remoting.impl.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.api.buffer.ByteBufferWrapper;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;
import org.apache.rocketmq.remoting.impl.buffer.NettyByteBufferWrapper;
import org.apache.rocketmq.remoting.impl.command.CodecHelper;
import org.apache.rocketmq.remoting.impl.protocol.serializer.SerializerFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Encoder extends MessageToByteEncoder<RemotingCommand> {
    private static final Logger LOG = LoggerFactory.getLogger(Encoder.class);

    private final SerializerFactory serializerFactory = new SerializerFactoryImpl();

    public Encoder() {
    }

    @Override
    public void encode(final ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
        try {
            ByteBufferWrapper wrapper = new NettyByteBufferWrapper(out);

            encode(serializerFactory, remotingCommand, wrapper);
        } catch (final Exception e) {
            LOG.error("Error occurred when encoding response for channel " + ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress(), e);
            if (remotingCommand != null) {
                LOG.error(remotingCommand.toString());
            }
            ctx.channel().close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOG.warn("Close channel {} because of error {},result is {}", ctx.channel(), e, future.isSuccess());
                }
            });
        }
    }

    public void encode(final SerializerFactory serializerFactory, final RemotingCommand remotingCommand,
        final ByteBufferWrapper out) {
        ByteBuffer encodeParameter = null;
        if (remotingCommand.parameterBytes() != null) {
            encodeParameter = ByteBuffer.wrap(remotingCommand.parameterBytes());
        } else if (remotingCommand.parameter() != null) {
            final Serializer serialization = serializerFactory.get(remotingCommand.serializerType());
            encodeParameter = serialization.encode(remotingCommand.parameter());
        }

        int parameterLength = encodeParameter != null ? encodeParameter.limit() : 0;
        int extBodyLength = remotingCommand.extraPayload() != null ? remotingCommand.extraPayload().length : 0;

        ByteBuffer header = CodecHelper.encodeHeader(remotingCommand, parameterLength, extBodyLength);
        out.writeBytes(header);

        if (encodeParameter != null) {
            out.writeBytes(encodeParameter);
        }

        if (remotingCommand.extraPayload() != null) {
            out.writeBytes(remotingCommand.extraPayload());
        }
    }
}
