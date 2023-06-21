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

import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HAProxyMessage) {
            HAProxyMessage message = (HAProxyMessage) msg;
            this.addProxyProtocolHeader(message, ctx.channel());
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        Stopwatch timer = Stopwatch.createStarted();
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            RemotingCommand cmd = RemotingCommand.decode(frame);
            cmd.setProcessTimer(timer);
            return cmd;
        } catch (Exception e) {
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingHelper.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }

    /**
     * The definition of key refers to the implementation of nginx
     * <a href="https://nginx.org/en/docs/http/ngx_http_core_module.html#var_proxy_protocol_addr">ngx_http_core_module</a>
     * @param msg
     * @param channel
     */
    private void addProxyProtocolHeader(HAProxyMessage msg, Channel channel) {
        if (StringUtils.isNotBlank(msg.sourceAddress())) {
            channel.attr(AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_ADDR)).set(msg.sourceAddress());
        }
        if (msg.sourcePort() > 0) {
            channel.attr(AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_PORT)).set(msg.sourcePort());
        }
        if (StringUtils.isNotBlank(msg.destinationAddress())) {
            channel.attr(AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_ADDR)).set(msg.destinationAddress());
        }
        if (msg.destinationPort() > 0) {
            channel.attr(AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_PORT)).set(msg.destinationPort());
        }
        if (CollectionUtils.isNotEmpty(msg.tlvs())) {
            msg.tlvs().forEach(tlv ->
                    channel.attr(AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX
                                    + String.format("%02x", tlv.typeByteValue())))
                            .set(tlv.content().toString(CharsetUtil.UTF_8)));
        }
    }
}
