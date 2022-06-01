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

package org.apache.rocketmq.proxy.service.channel;

import com.google.common.base.Strings;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * SimpleChannel is used to handle writeAndFlush situation in processor
 *
 * @see io.netty.channel.ChannelHandlerContext#writeAndFlush
 * @see io.netty.channel.Channel#writeAndFlush
 */
public class SimpleChannel extends AbstractChannel {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected final String remoteAddress;
    protected final String localAddress;

    protected long lastAccessTime;
    protected ChannelHandlerContext channelHandlerContext;

    /**
     * Creates a new instance.
     *
     * @param parent        the parent of this channel. {@code null} if there's no parent.
     * @param remoteAddress Remote address
     * @param localAddress  Local address
     */
    public SimpleChannel(Channel parent, String remoteAddress, String localAddress) {
        this(parent, null, remoteAddress, localAddress);
    }

    public SimpleChannel(Channel parent, ChannelId id, String remoteAddress, String localAddress) {
        super(parent, id);
        lastAccessTime = System.currentTimeMillis();
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.channelHandlerContext = new SimpleChannelHandlerContext(this);
    }

    public SimpleChannel(String remoteAddress, String localAddress) {
        this(null, remoteAddress, localAddress);
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
    public ChannelFuture writeAndFlush(Object msg) {
        DefaultChannelPromise promise = new DefaultChannelPromise(this, GlobalEventExecutor.INSTANCE);
        promise.setSuccess();
        return promise;
    }

    public void updateLastAccessTime() {
        this.lastAccessTime = System.currentTimeMillis();
    }

    public void registerInvocationContext(int opaque, InvocationContextInterface context) {

    }

    public void eraseInvocationContext(int opaque) {

    }

    public void clearExpireContext() {

    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }
}