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
package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.ChannelHandlerContext;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.Channel;

public class PopRequest {
    private static final AtomicLong COUNTER = new AtomicLong(Long.MIN_VALUE);

    private final RemotingCommand remotingCommand;
    private final ChannelHandlerContext ctx;
    private final long expired;
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final long op = COUNTER.getAndIncrement();

    public PopRequest(RemotingCommand remotingCommand, ChannelHandlerContext ctx, long expired) {
        this.ctx = ctx;
        this.remotingCommand = remotingCommand;
        this.expired = expired;
    }

    public Channel getChannel() {
        return ctx.channel();
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public RemotingCommand getRemotingCommand() {
        return remotingCommand;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() > (expired - 50);
    }

    public boolean complete() {
        return complete.compareAndSet(false, true);
    }

    public long getExpired() {
        return expired;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PopRequest{");
        sb.append("cmd=").append(remotingCommand);
        sb.append(", ctx=").append(ctx);
        sb.append(", expired=").append(expired);
        sb.append(", complete=").append(complete);
        sb.append(", op=").append(op);
        sb.append('}');
        return sb.toString();
    }

    public static final Comparator<PopRequest> COMPARATOR = (o1, o2) -> {
        int ret = (int) (o1.getExpired() - o2.getExpired());

        if (ret != 0) {
            return ret;
        }
        ret = (int) (o1.op - o2.op);
        if (ret != 0) {
            return ret;
        }
        return -1;
    };
}
