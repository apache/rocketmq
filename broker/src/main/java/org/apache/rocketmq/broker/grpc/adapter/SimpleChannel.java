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

import com.google.common.base.Strings;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleChannel extends AbstractChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final String remoteAddress;
    private final String localAddress;

    private long lastAccessTime;

    protected final ConcurrentMap<Integer, InvocationContext> inFlightRequestMap;

    /**
     * Creates a new instance.
     *
     * @param parent the parent of this channel. {@code null} if there's no parent.
     * @param remoteAddress Remote address
     * @param localAddress Local address
     */
    public SimpleChannel(Channel parent, String remoteAddress, String localAddress) {
        super(parent);
        lastAccessTime = System.currentTimeMillis();
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.inFlightRequestMap = new ConcurrentHashMap<>();
    }

    public SimpleChannel(SimpleChannel other) {
        super(other);
        lastAccessTime = System.currentTimeMillis();
        this.remoteAddress = other.remoteAddress;
        this.localAddress = other.localAddress;
        this.inFlightRequestMap = other.inFlightRequestMap;
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return null;
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return false;
    }

    private static SocketAddress parseSocketAddress(String address) {
        if (Strings.isNullOrEmpty(address)) {
            return null;
        }

        String[] segments = address.split(":");
        if (2 == segments.length) {
            return new InetSocketAddress(segments[0], Integer.parseInt(segments[1]));
        }

        return null;
    }

    @Override
    protected SocketAddress localAddress0() {
        return parseSocketAddress(localAddress);
    }

    @Override
    public SocketAddress localAddress() {
        return localAddress0();
    }

    @Override
    public SocketAddress remoteAddress() {
        return remoteAddress0();
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return parseSocketAddress(remoteAddress);
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {

    }

    @Override
    protected void doDisconnect() throws Exception {

    }

    @Override
    public ChannelFuture close() {
        DefaultChannelPromise promise = new DefaultChannelPromise(this, GlobalEventExecutor.INSTANCE);
        promise.setSuccess();
        return promise;
    }

    @Override
    protected void doClose() throws Exception {

    }

    @Override
    protected void doBeginRead() throws Exception {

    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {

    }

    public boolean isWritable(int opaque) {
        if (!inFlightRequestMap.containsKey(opaque)) {
            return false;
        }

        InvocationContext invocationContext = inFlightRequestMap.get(opaque);
        if (null != invocationContext) {
            return null != invocationContext.getStreamObserver() && invocationContext.getStreamObserver().isReady();
        }
        return false;
    }

    @Override
    public ChannelConfig config() {
        return null;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean isActive() {
        return (System.currentTimeMillis() - lastAccessTime) <= 120L * 1000;
    }

    @Override
    public ChannelMetadata metadata() {
        return null;
    }

    @Override
    public EventLoop eventLoop() {
        return super.eventLoop();
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        if (msg instanceof RemotingCommand) {
            RemotingCommand responseCommand = (RemotingCommand) msg;
            inFlightRequestMap.remove(responseCommand.getOpaque());
        }

        DefaultChannelPromise promise = new DefaultChannelPromise(this, GlobalEventExecutor.INSTANCE);
        promise.setSuccess();
        return promise;
    }

    public void registerInvocationContext(int opaque, InvocationContext context) {
        inFlightRequestMap.put(opaque, context);
    }

    public void eraseInvocationContext(int opaque) {
        inFlightRequestMap.remove(opaque);
    }

    public void cleanExpiredRequests() {
        Iterator<Map.Entry<Integer, InvocationContext>> iterator = inFlightRequestMap.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<Integer, InvocationContext> entry = iterator.next();
            if (entry.getValue().expired()) {
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

    public SimpleChannel updateLastAccessTime() {
        lastAccessTime = System.currentTimeMillis();
        return this;
    }
}
