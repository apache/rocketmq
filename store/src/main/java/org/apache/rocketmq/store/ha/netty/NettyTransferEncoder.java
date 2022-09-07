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

package org.apache.rocketmq.store.ha.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.ByteBuffer;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

@ChannelHandler.Sharable
public class NettyTransferEncoder extends MessageToByteEncoder<TransferMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    @Override
    protected void encode(ChannelHandlerContext ctx, TransferMessage message, ByteBuf out) throws Exception {
        if (message == null || message.getType() == null) {
            String errorMessage = "The encode message is null or message type not exist";
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        out.writeInt(message.getBodyLength());
        out.writeInt(message.getType().getValue());
        out.writeLong(message.getEpoch());
        out.writeLong(System.currentTimeMillis());
        for (ByteBuffer byteBuffer : message.getByteBufferList()) {
            out.writeBytes(byteBuffer);
        }
    }
}