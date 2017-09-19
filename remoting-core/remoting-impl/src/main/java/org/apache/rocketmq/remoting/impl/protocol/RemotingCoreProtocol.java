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
import org.apache.rocketmq.remoting.api.channel.ChannelHandlerContextWrapper;
import org.apache.rocketmq.remoting.api.protocol.Protocol;
import org.apache.rocketmq.remoting.impl.netty.handler.Decoder;
import org.apache.rocketmq.remoting.impl.netty.handler.Encoder;
import org.apache.rocketmq.remoting.impl.netty.handler.ProtocolSelector;

public class RemotingCoreProtocol implements Protocol {
    @Override
    public String name() {
        return MVP;
    }

    @Override
    public byte type() {
        return MVP_MAGIC;
    }

    @Override
    public void assembleHandler(final ChannelHandlerContextWrapper ctx) {

        ChannelHandlerContext chx = (ChannelHandlerContext) ctx.getContext();

        chx.pipeline().addAfter(ProtocolSelector.NAME, "decoder", new Decoder());
        chx.pipeline().addAfter("decoder", "encoder", new Encoder());
    }
}
