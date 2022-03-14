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

package org.apache.rocketmq.proxy.channel;

import io.netty.channel.ChannelFuture;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.proxy.grpc.adapter.InvocationContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class InvocationChannel<R, W> extends SimpleChannel {
    protected final ConcurrentMap<Integer, InvocationContext<R, W>> inFlightRequestMap;

    public InvocationChannel(SimpleChannel simpleChannel) {
        super(simpleChannel);
        this.inFlightRequestMap = new ConcurrentHashMap<>();
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        if (msg instanceof RemotingCommand) {
            RemotingCommand responseCommand = (RemotingCommand) msg;
            inFlightRequestMap.remove(responseCommand.getOpaque());
        }
        return super.writeAndFlush(msg);
    }

    public boolean isWritable(int opaque) {
        if (!inFlightRequestMap.containsKey(opaque)) {
            return false;
        }

        InvocationContext<R, W> invocationContext = inFlightRequestMap.get(opaque);
        if (null != invocationContext) {
            CompletableFuture<?> future = invocationContext.getResponse();
            return null != future && !future.isCancelled() && !future.isCompletedExceptionally() && !future.isDone();
        }
        return false;
    }

    public void registerInvocationContext(int opaque, InvocationContext<R, W> context) {
        inFlightRequestMap.put(opaque, context);
    }

    public void eraseInvocationContext(int opaque) {
        inFlightRequestMap.remove(opaque);
    }

    public void cleanExpiredRequests() {
        Iterator<Map.Entry<Integer, InvocationContext<R, W>>> iterator = inFlightRequestMap.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer, InvocationContext<R, W>> entry = iterator.next();
            if (entry.getValue().expired(expiredTimeSec)) {
                iterator.remove();
                count++;
                LOGGER.debug("An expired request is found, created time-point: {}, Request: {}",
                    entry.getValue().getTimestamp(), entry.getValue().getRequest());
            }
        }
        if (count > 0) {
            LOGGER.warn("[BUG] {} expired in-flight requests is cleaned.", count);
        }
    }
}
