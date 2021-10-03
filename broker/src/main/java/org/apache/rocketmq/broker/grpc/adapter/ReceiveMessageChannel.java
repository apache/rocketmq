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

package org.apache.rocketmq.broker.grpc.adapter;

import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ReceiveMessageChannel extends SimpleChannel {
    /**
     * Creates a new instance.
     *
     * @param parent the parent of this channel. {@code null} if there's no parent.
     * @param remoteAddress Remote address
     * @param localAddress Local address
     */
    public ReceiveMessageChannel(Channel parent, String remoteAddress, String localAddress) {
        super(parent, remoteAddress, localAddress);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        if (msg instanceof RemotingCommand) {
            RemotingCommand responseCommand = (RemotingCommand) msg;
            InvocationContext<ReceiveMessageRequest, ReceiveMessageResponse> context = inFlightRequestMap.get(responseCommand.getOpaque());
            if (null != context) {
                controller.getBrokerGrpcService()
                    .handlePopResponseCommand(responseCommand, context);
            }
        }
        return super.writeAndFlush(msg);
    }
}
