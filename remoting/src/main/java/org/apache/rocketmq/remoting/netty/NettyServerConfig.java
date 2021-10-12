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
    private int listenPort = 8888;
    private int serverWorkerThreads = 8;
    private int serverCallbackExecutorThreads = 0;
    private int serverSelectorThreads = 3;
    private int serverOnewaySemaphoreValue = 256;
    private int serverAsyncSemaphoreValue = 64;
    private int serverChannelMaxIdleTimeSeconds = 120;

    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * when is true, will use 4 bytes to judge protocol
     * <p>
     * nowadays, used in namesrv to to judge the protocol of requests is http2 or remoting
     */
    private boolean enableProtocolNegotiation = true;
    /**
     * when both enableHttp2Proxy and enableProtocolNegotiation are true,
     * the requests of http2 will be proxied to the backend server (configured by http2ProxyHost and http2ProxyPort)
     * <p>
     * nowadays, used in namesrv to support grpc protocol
     */
    private boolean enableHttp2Proxy = true;
    /**
     * the backend http2 server is enable ssl or not
     */
    private boolean enableHttp2SslProxy = true;
    private String http2ProxyHost = "127.0.0.1";

    /**
     * If inheritGrpcPortToHTTP2 is set, then {@link #http2ProxyPort} will be ignored.
     * The value port in GrpcServerConfig will be used.
     * @see org.apache.rocketmq.grpc.server.GrpcServerConfig#getPort()
     */
    private boolean inheritGrpcPortToHTTP2 = true;
    private int http2ProxyPort = 10922;

    /**
     * make make install
     *
     *
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
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

    public boolean isEnableProtocolNegotiation() {
        return enableProtocolNegotiation;
    }

    public void setEnableProtocolNegotiation(boolean enableProtocolNegotiation) {
        this.enableProtocolNegotiation = enableProtocolNegotiation;
    }

    public boolean isEnableHttp2Proxy() {
        return enableHttp2Proxy;
    }

    public void setEnableHttp2Proxy(boolean enableHttp2Proxy) {
        this.enableHttp2Proxy = enableHttp2Proxy;
    }

    public boolean isEnableHttp2SslProxy() {
        return enableHttp2SslProxy;
    }

    public void setEnableHttp2SslProxy(boolean enableHttp2SslProxy) {
        this.enableHttp2SslProxy = enableHttp2SslProxy;
    }

    public String getHttp2ProxyHost() {
        return http2ProxyHost;
    }

    public void setHttp2ProxyHost(String http2ProxyHost) {
        this.http2ProxyHost = http2ProxyHost;
    }

    public int getHttp2ProxyPort() {
        return http2ProxyPort;
    }

    public void setHttp2ProxyPort(int http2ProxyPort) {
        this.http2ProxyPort = http2ProxyPort;
    }

    public boolean isInheritGrpcPortToHTTP2() {
        return inheritGrpcPortToHTTP2;
    }

    public void setInheritGrpcPortToHTTP2(boolean inheritGrpcPortToHTTP2) {
        this.inheritGrpcPortToHTTP2 = inheritGrpcPortToHTTP2;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }
}
