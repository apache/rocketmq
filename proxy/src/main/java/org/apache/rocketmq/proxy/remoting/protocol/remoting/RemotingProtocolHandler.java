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

package org.apache.rocketmq.proxy.remoting.protocol.remoting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.remoting.protocol.ProtocolHandler;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.RemotingCodeDistributionHandler;

public class RemotingProtocolHandler implements ProtocolHandler {

    private final Supplier<NettyEncoder> encoderSupplier;
    private final Supplier<RemotingCodeDistributionHandler> remotingCodeDistributionHandlerSupplier;
    private final Supplier<NettyRemotingServer.NettyConnectManageHandler> connectionManageHandlerSupplier;
    private final Supplier<NettyRemotingServer.NettyServerHandler> serverHandlerSupplier;

    public RemotingProtocolHandler(Supplier<NettyEncoder> encoderSupplier,
        Supplier<RemotingCodeDistributionHandler> remotingCodeDistributionHandlerSupplier,
        Supplier<NettyRemotingServer.NettyConnectManageHandler> connectionManageHandlerSupplier,
        Supplier<NettyRemotingServer.NettyServerHandler> serverHandlerSupplier) {
        this.encoderSupplier = encoderSupplier;
        this.remotingCodeDistributionHandlerSupplier = remotingCodeDistributionHandlerSupplier;
        this.connectionManageHandlerSupplier = connectionManageHandlerSupplier;
        this.serverHandlerSupplier = serverHandlerSupplier;
    }

    @Override
    public boolean match(ByteBuf in) {
        return true;
    }

    @Override
    public void config(ChannelHandlerContext ctx, ByteBuf msg) {
        ctx.pipeline().addLast(
            this.encoderSupplier.get(),
            new NettyDecoder(),
            this.remotingCodeDistributionHandlerSupplier.get(),
            this.connectionManageHandlerSupplier.get(),
            this.serverHandlerSupplier.get()
        );
    }
}
