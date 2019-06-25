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

public class NettyServerConfig implements Cloneable {
    // NameServer监昕端口，该值默认会被初始化为 98760
    private int listenPort = 8888;
    // Netty业务线程池线程个数
    private int serverWorkerThreads = 8;
    //Netty public任务线程池线程个数， Netty 网络设计，
    //根据业务类型会创建不同的线程池，比如处理消息发送、消息消费、心跳检测等 。
    //如果该业务类型(RequestCode)未注册线程池， 则由 public线程池执行。
    private int serverCallbackExecutorThreads = 0;
    //IO线程池线程个数，主要是 NameServer、Broker端解析请求、 返回相应的线程个数，这 类线程主要是 处理网 络请求的，解 析请求包， 然后 转发到
    //各个业务线程池完成具体的业务操作，然后将结果再返回调用方 。
    private int serverSelectorThreads = 3;
    //send oneway 消息请求井发度( Broker 端参数)
    private int serverOnewaySemaphoreValue = 256;
    // 异步消息发送最大并发度( Broker 端参数) 。
    private int serverAsyncSemaphoreValue = 64;
    //网 络连接最大空 闲时间，默认 120s。 如果连接
    //空闲 时间 超过该参数设置的值，连接将被关闭 。
    private int serverChannelMaxIdleTimeSeconds = 120;

    // 网络 socket发送缓存区大小， 默认 64k。
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    // 网络 socket接收缓存区大小 ，默认 64k。
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    // ByteBuffer是否开启缓存 ， 建议开启
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * make make install
     *
     *
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    // 是否启用EpollIO模型， Linux环境建议开启
    private boolean useEpollNativeSelector = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }
}
