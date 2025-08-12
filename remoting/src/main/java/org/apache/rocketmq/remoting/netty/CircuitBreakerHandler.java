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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.CircuitBreakerCounter;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.CircuitBreakerException;

import java.net.ConnectException;
import java.net.SocketAddress;

public class CircuitBreakerHandler extends ChannelOutboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    private final CircuitBreakerCounter counter;

    private static final String UNKNOWN = "UNKNOWN";

    public CircuitBreakerHandler(CircuitBreakerCounter counter) {
        this.counter = counter;
    }

    @Override
    public void connect(ChannelHandlerContext ctx,
                        SocketAddress remoteAddress,
                        SocketAddress localAddress,
                        ChannelPromise promise) throws Exception {

        final String remote = remoteAddress == null ? UNKNOWN : RemotingHelper.parseSocketAddressAddr(remoteAddress);
        if (remote.equals(UNKNOWN)) {
            super.connect(ctx, remoteAddress, localAddress, promise);
            return;
        }

        if (!counter.check(remote)) {
            log.warn("CircuitBreaker: [{}]Too many failed connection attempts, connection denied", remote);
            throw new CircuitBreakerException(remote);
        }
        promise.addListener((ChannelFuture f) -> {
            if (!f.isSuccess() && f.cause() instanceof ConnectException) {
                counter.addCount(remote, 1);
            }
        });
        super.connect(ctx, remoteAddress, localAddress, promise);
    }
}
