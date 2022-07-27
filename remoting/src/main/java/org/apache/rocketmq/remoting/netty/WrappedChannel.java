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

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * <p>Netty's Channel is way too powerful and is easy to be incorrectly used, such as calling writeAndFlush
 * in the Processor resulting the break of hook lifecycle.</p>
 *
 * <p>Please use WrappedChannel in NettyRequestProcessor instead of Channel.</p>
 */
public class WrappedChannel {
    private final Channel channel;

    public WrappedChannel(Channel channel) {
        this.channel = channel;
    }

    public TriConsumer<Integer, Integer, String> getFastFailCallback() {
        return (opaque, code, remark) -> {
            final RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
            response.setOpaque(opaque);
            WrappedChannel.this.channel.writeAndFlush(response);
        };
    }

    public void closeChannel() {
        if (this.channel != null) {
            RemotingUtil.closeChannel(this.channel);
        }
    }

    public boolean isSameChannel(Channel channel) {
        return this.channel == channel;
    }

    public String info() {
        return this.channel.toString();
    }
}
