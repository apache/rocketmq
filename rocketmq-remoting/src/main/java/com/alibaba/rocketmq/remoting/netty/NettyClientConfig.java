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
package com.alibaba.rocketmq.remoting.netty;

/**
 * Netty客户端配置类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class NettyClientConfig {
    // 处理Server Response/Request
    private int clientWorkerThreads = 4;
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private int clientSelectorThreads = 1;
    private int clientOnewaySemaphoreValue = 256;
    private int clientAsyncSemaphoreValue = 128;
    private long connectTimeoutMillis = 3000;
    // channel超过1分钟不被访问 就关闭
    private long channelNotActiveInterval = 1000 * 60;

    private int clientChannelMaxIdleTimeSeconds = 120;


    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }


    public void setClientWorkerThreads(int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }


    public int getClientSelectorThreads() {
        return clientSelectorThreads;
    }


    public void setClientSelectorThreads(int clientSelectorThreads) {
        this.clientSelectorThreads = clientSelectorThreads;
    }


    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }


    public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }


    public long getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }


    public void setConnectTimeoutMillis(long connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }


    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }


    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }


    public long getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }


    public void setChannelNotActiveInterval(long channelNotActiveInterval) {
        this.channelNotActiveInterval = channelNotActiveInterval;
    }


    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }


    public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }


    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }


    public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
        this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
    }
}
