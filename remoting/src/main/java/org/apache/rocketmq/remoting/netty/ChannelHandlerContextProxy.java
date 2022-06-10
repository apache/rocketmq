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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.net.SocketAddress;

public class ChannelHandlerContextProxy implements ChannelHandlerContext {

    private final ChannelHandlerContext proxy;
    private final RemotingResponseCallback callback;

    public ChannelHandlerContextProxy(ChannelHandlerContext proxy, RemotingResponseCallback callback) {
        this.proxy = proxy;
        this.callback = callback;
    }

    @Override
    public Channel channel() {
        return new ChannelProxy(proxy.channel(), callback);
    }

    @Override
    public EventExecutor executor() {
        return proxy.executor();
    }

    @Override
    public String name() {
        return proxy.name();
    }

    @Override
    public ChannelHandler handler() {
        return proxy.handler();
    }

    @Override
    public boolean isRemoved() {
        return proxy.isRemoved();
    }

    @Override
    public ChannelHandlerContext fireChannelRegistered() {
        return proxy.fireChannelRegistered();
    }

    @Override
    public ChannelHandlerContext fireChannelUnregistered() {
        return proxy.fireChannelUnregistered();
    }

    @Override
    public ChannelHandlerContext fireChannelActive() {
        return proxy.fireChannelActive();
    }

    @Override
    public ChannelHandlerContext fireChannelInactive() {
        return proxy.fireChannelInactive();
    }

    @Override
    public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
        return proxy.fireExceptionCaught(cause);
    }

    @Override
    public ChannelHandlerContext fireUserEventTriggered(Object evt) {
        return proxy.fireUserEventTriggered(evt);
    }

    @Override
    public ChannelHandlerContext fireChannelRead(Object msg) {
        return proxy.fireChannelRead(msg);
    }

    @Override
    public ChannelHandlerContext fireChannelReadComplete() {
        return proxy.fireChannelReadComplete();
    }

    @Override
    public ChannelHandlerContext fireChannelWritabilityChanged() {
        return proxy.fireChannelWritabilityChanged();
    }

    @Override
    public ChannelHandlerContext read() {
        return proxy.read();
    }

    @Override
    public ChannelHandlerContext flush() {
        return proxy.flush();
    }

    @Override
    public ChannelPipeline pipeline() {
        return proxy.pipeline();
    }

    @Override
    public ByteBufAllocator alloc() {
        return proxy.alloc();
    }

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
        return proxy.attr(key);
    }

    @Override
    public <T> boolean hasAttr(AttributeKey<T> key) {
        return proxy.hasAttr(key);
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return proxy.bind(localAddress);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return proxy.connect(remoteAddress);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return proxy.connect(remoteAddress, localAddress);
    }

    @Override
    public ChannelFuture disconnect() {
        return proxy.disconnect();
    }

    @Override
    public ChannelFuture close() {
        return proxy.close();
    }

    @Override
    public ChannelFuture deregister() {
        return proxy.deregister();
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return proxy.bind(localAddress, promise);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return proxy.connect(remoteAddress, promise);
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        return proxy.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise promise) {
        return proxy.disconnect(promise);
    }

    @Override
    public ChannelFuture close(ChannelPromise promise) {
        return proxy.close(promise);
    }

    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
        return proxy.deregister(promise);
    }

    @Override
    public ChannelFuture write(Object msg) {
        RemotingCommand response = (RemotingCommand)msg;
        callback.callback(response);
        return proxy.write(msg);
    }

    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        RemotingCommand response = (RemotingCommand)msg;
        callback.callback(response);
        return proxy.write(msg, promise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        RemotingCommand response = (RemotingCommand)msg;
        callback.callback(response);
        return proxy.writeAndFlush(msg, promise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        RemotingCommand response = (RemotingCommand)msg;
        callback.callback(response);
        return proxy.writeAndFlush(msg);
    }

    @Override
    public ChannelPromise newPromise() {
        return proxy.newPromise();
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return proxy.newProgressivePromise();
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        return proxy.newSucceededFuture();
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
        return proxy.newFailedFuture(cause);
    }

    @Override
    public ChannelPromise voidPromise() {
        return proxy.voidPromise();
    }
}
