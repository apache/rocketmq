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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.proxy.common.Cleaner;
import org.apache.rocketmq.proxy.grpc.v2.adapter.handler.ResponseHandler;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public abstract class InvocationChannel<R, W> extends SimpleChannel implements Cleaner {
    protected final ConcurrentMap<Integer, InvocationContext<R, W>> inFlightRequestMap;
    protected final ResponseHandler<R, W> handler;

    public InvocationChannel(ResponseHandler<R, W> handler) {
        super(ChannelManager.createSimpleChannelDirectly());
        this.inFlightRequestMap = new ConcurrentHashMap<>();
        this.handler = handler;
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        if (msg instanceof RemotingCommand) {
            RemotingCommand responseCommand = (RemotingCommand) msg;
            InvocationContext<R, W> context = inFlightRequestMap.remove(responseCommand.getOpaque());
            if (null != context) {
                handler.handle(responseCommand, context);
            }
            inFlightRequestMap.remove(responseCommand.getOpaque());
        }
        return super.writeAndFlush(msg);
    }

    @Override
    public boolean isWritable() {
        return inFlightRequestMap.size() > 0;
    }

    public void registerInvocationContext(int opaque, InvocationContext<R, W> context) {
        inFlightRequestMap.put(opaque, context);
    }

    public void eraseInvocationContext(int opaque) {
        inFlightRequestMap.remove(opaque);
    }

    @Override
    public void clean() {
        Iterator<Map.Entry<Integer, InvocationContext<R, W>>> iterator = inFlightRequestMap.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer, InvocationContext<R, W>> entry = iterator.next();
            if (entry.getValue().expired(expiredTimeSec)) {
                iterator.remove();
                count++;
                log.debug("An expired request is found, created time-point: {}, Request: {}",
                    entry.getValue().getTimestamp(), entry.getValue().getRequest());
            }
        }
        if (count > 0) {
            log.warn("[BUG] {} expired in-flight requests is cleaned.", count);
        }
    }
}
