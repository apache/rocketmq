/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.longpolling;

import io.netty.channel.Channel;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * 一个拉消息请求
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class PullRequest {
    private final RemotingCommand requestCommand;
    private final Channel clientChannel;
    private final long timeoutMillis;
    private final long suspendTimestamp;
    private final long pullFromThisOffset;


    public PullRequest(RemotingCommand requestCommand, Channel clientChannel, long timeoutMillis,
            long suspendTimestamp, long pullFromThisOffset) {
        this.requestCommand = requestCommand;
        this.clientChannel = clientChannel;
        this.timeoutMillis = timeoutMillis;
        this.suspendTimestamp = suspendTimestamp;
        this.pullFromThisOffset = pullFromThisOffset;
    }


    public RemotingCommand getRequestCommand() {
        return requestCommand;
    }


    public Channel getClientChannel() {
        return clientChannel;
    }


    public long getTimeoutMillis() {
        return timeoutMillis;
    }


    public long getSuspendTimestamp() {
        return suspendTimestamp;
    }


    public long getPullFromThisOffset() {
        return pullFromThisOffset;
    }
}
