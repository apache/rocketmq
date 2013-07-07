/**
 * $Id: NettyClientConfig.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.netty;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class NettyClientConfig {
    // 处理Server Response/Request
    private int clientWorkerThreads = 4;
    private int clientCallbackExecutorThreads = 4;
    private int clientSelectorThreads = 1;
    private int clientOnewaySemaphoreValue = 128;
    private int clientAsyncSemaphoreValue = 256;
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
