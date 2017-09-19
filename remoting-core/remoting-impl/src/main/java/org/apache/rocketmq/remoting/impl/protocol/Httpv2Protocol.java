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

package org.apache.rocketmq.remoting.impl.protocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.remoting.api.channel.ChannelHandlerContextWrapper;
import org.apache.rocketmq.remoting.api.protocol.Protocol;
import org.apache.rocketmq.remoting.impl.netty.handler.Http2Handler;
import org.apache.rocketmq.remoting.impl.netty.handler.ProtocolSelector;

public class Httpv2Protocol extends RemotingCoreProtocol {
    private SslContext sslContext;

    public Httpv2Protocol(final SslContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public String name() {
        return Protocol.HTTP2;
    }

    @Override
    public byte type() {
        return Protocol.HTTP_2_MAGIC;
    }

    @Override
    public void assembleHandler(final ChannelHandlerContextWrapper ctx) {
        super.assembleHandler(ctx);
        ChannelHandlerContext chx = (ChannelHandlerContext) ctx.getContext();

        chx.pipeline().addAfter(ProtocolSelector.NAME, "sslHandler", sslContext.newHandler(chx.alloc()));
        chx.pipeline().addAfter("sslHandler", "http2Handler", Http2Handler.newHandler(true));
    }

}
