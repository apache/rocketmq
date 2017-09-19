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

package org.apache.rocketmq.remoting.config;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.remoting.api.protocol.Protocol;
import org.apache.rocketmq.remoting.impl.protocol.compression.GZipCompressor;
import org.apache.rocketmq.remoting.impl.protocol.serializer.MsgPackSerializer;

public class RemotingConfig extends TcpSocketConfig {
    private int connectionMaxRetries = 3;
    private int connectionChannelReaderIdleSeconds = 0;
    private int connectionChannelWriterIdleSeconds = 0;
    /**
     * IdleStateEvent will be triggered when neither read nor write was
     * performed for the specified period of this time. Specify {@code 0} to
     * disable
     */
    private int connectionChannelIdleSeconds = 120;
    private int writeBufLowWaterMark = 32 * 10240;
    private int writeBufHighWaterMark = 64 * 10240;
    private int threadTaskLowWaterMark = 30000;
    private int threadTaskHighWaterMark = 50000;
    private int connectionRetryBackoffMillis = 3000;
    private String protocolName = Protocol.MVP;
    private String serializerName = MsgPackSerializer.SERIALIZER_NAME;
    private String compressorName = GZipCompressor.COMPRESSOR_NAME;
    private int serviceThreadBlockQueueSize = 50000;
    private boolean clientNativeEpollEnable = false;
    private int clientWorkerThreads = 16 + Runtime.getRuntime().availableProcessors() * 2;
    private int clientConnectionFutureAwaitTimeoutMillis = 30000;
    private int clientAsyncCallbackExecutorThreads = 16 + Runtime.getRuntime().availableProcessors() * 2;
    private int clientOnewayInvokeSemaphore = 20480;

    //=============Server configuration==================
    private int clientAsyncInvokeSemaphore = 20480;
    private boolean clientPooledBytebufAllocatorEnable = false;
    private boolean clientCloseSocketIfTimeout = true;
    private boolean clientShortConnectionEnable = false;
    private long clientPublishServiceTimeout = 10000;
    private long clientConsumerServiceTimeout = 10000;
    private long clientInvokeServiceTimeout = 10000;
    private int clientMaxRetryCount = 10;
    private int clientSleepBeforeRetry = 100;
    private int serverListenPort = 8888;
    /**
     * If server only listened 1 port,recommend to set the value to 1
     */
    private int serverAcceptorThreads = 1;
    private int serverIoThreads = 16 + Runtime.getRuntime().availableProcessors() * 2;
    private int serverWorkerThreads = 16 + Runtime.getRuntime().availableProcessors() * 2;
    private int serverOnewayInvokeSemaphore = 256;
    private int serverAsyncInvokeSemaphore = 6400;
    private boolean serverNativeEpollEnable = false;
    private int serverAsyncCallbackExecutorThreads = Runtime.getRuntime().availableProcessors() * 2;
    private boolean serverPooledBytebufAllocatorEnable = true;
    private boolean serverAuthOpenEnable = true;

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public int getConnectionMaxRetries() {
        return connectionMaxRetries;
    }

    public void setConnectionMaxRetries(final int connectionMaxRetries) {
        this.connectionMaxRetries = connectionMaxRetries;
    }

    public int getConnectionChannelReaderIdleSeconds() {
        return connectionChannelReaderIdleSeconds;
    }

    public void setConnectionChannelReaderIdleSeconds(final int connectionChannelReaderIdleSeconds) {
        this.connectionChannelReaderIdleSeconds = connectionChannelReaderIdleSeconds;
    }

    public int getConnectionChannelWriterIdleSeconds() {
        return connectionChannelWriterIdleSeconds;
    }

    public void setConnectionChannelWriterIdleSeconds(final int connectionChannelWriterIdleSeconds) {
        this.connectionChannelWriterIdleSeconds = connectionChannelWriterIdleSeconds;
    }

    public int getConnectionChannelIdleSeconds() {
        return connectionChannelIdleSeconds;
    }

    public void setConnectionChannelIdleSeconds(final int connectionChannelIdleSeconds) {
        this.connectionChannelIdleSeconds = connectionChannelIdleSeconds;
    }

    public int getWriteBufLowWaterMark() {
        return writeBufLowWaterMark;
    }

    public void setWriteBufLowWaterMark(final int writeBufLowWaterMark) {
        this.writeBufLowWaterMark = writeBufLowWaterMark;
    }

    public int getWriteBufHighWaterMark() {
        return writeBufHighWaterMark;
    }

    public void setWriteBufHighWaterMark(final int writeBufHighWaterMark) {
        this.writeBufHighWaterMark = writeBufHighWaterMark;
    }

    public int getThreadTaskLowWaterMark() {
        return threadTaskLowWaterMark;
    }

    public void setThreadTaskLowWaterMark(final int threadTaskLowWaterMark) {
        this.threadTaskLowWaterMark = threadTaskLowWaterMark;
    }

    public int getThreadTaskHighWaterMark() {
        return threadTaskHighWaterMark;
    }

    public void setThreadTaskHighWaterMark(final int threadTaskHighWaterMark) {
        this.threadTaskHighWaterMark = threadTaskHighWaterMark;
    }

    public int getConnectionRetryBackoffMillis() {
        return connectionRetryBackoffMillis;
    }

    public void setConnectionRetryBackoffMillis(final int connectionRetryBackoffMillis) {
        this.connectionRetryBackoffMillis = connectionRetryBackoffMillis;
    }

    public String getProtocolName() {
        return protocolName;
    }

    public void setProtocolName(final String protocolName) {
        this.protocolName = protocolName;
    }

    public String getSerializerName() {
        return serializerName;
    }

    public void setSerializerName(final String serializerName) {
        this.serializerName = serializerName;
    }

    public String getCompressorName() {
        return compressorName;
    }

    public void setCompressorName(final String compressorName) {
        this.compressorName = compressorName;
    }

    public int getServiceThreadBlockQueueSize() {
        return serviceThreadBlockQueueSize;
    }

    public void setServiceThreadBlockQueueSize(final int serviceThreadBlockQueueSize) {
        this.serviceThreadBlockQueueSize = serviceThreadBlockQueueSize;
    }

    public boolean isClientNativeEpollEnable() {
        return clientNativeEpollEnable;
    }

    public void setClientNativeEpollEnable(final boolean clientNativeEpollEnable) {
        this.clientNativeEpollEnable = clientNativeEpollEnable;
    }

    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }

    public void setClientWorkerThreads(final int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }

    public int getClientConnectionFutureAwaitTimeoutMillis() {
        return clientConnectionFutureAwaitTimeoutMillis;
    }

    public void setClientConnectionFutureAwaitTimeoutMillis(final int clientConnectionFutureAwaitTimeoutMillis) {
        this.clientConnectionFutureAwaitTimeoutMillis = clientConnectionFutureAwaitTimeoutMillis;
    }

    public int getClientAsyncCallbackExecutorThreads() {
        return clientAsyncCallbackExecutorThreads;
    }

    public void setClientAsyncCallbackExecutorThreads(final int clientAsyncCallbackExecutorThreads) {
        this.clientAsyncCallbackExecutorThreads = clientAsyncCallbackExecutorThreads;
    }

    public int getClientOnewayInvokeSemaphore() {
        return clientOnewayInvokeSemaphore;
    }

    public void setClientOnewayInvokeSemaphore(final int clientOnewayInvokeSemaphore) {
        this.clientOnewayInvokeSemaphore = clientOnewayInvokeSemaphore;
    }

    public int getClientAsyncInvokeSemaphore() {
        return clientAsyncInvokeSemaphore;
    }

    public void setClientAsyncInvokeSemaphore(final int clientAsyncInvokeSemaphore) {
        this.clientAsyncInvokeSemaphore = clientAsyncInvokeSemaphore;
    }

    public boolean isClientPooledBytebufAllocatorEnable() {
        return clientPooledBytebufAllocatorEnable;
    }

    public void setClientPooledBytebufAllocatorEnable(final boolean clientPooledBytebufAllocatorEnable) {
        this.clientPooledBytebufAllocatorEnable = clientPooledBytebufAllocatorEnable;
    }

    public boolean isClientCloseSocketIfTimeout() {
        return clientCloseSocketIfTimeout;
    }

    public void setClientCloseSocketIfTimeout(final boolean clientCloseSocketIfTimeout) {
        this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
    }

    public boolean isClientShortConnectionEnable() {
        return clientShortConnectionEnable;
    }

    public void setClientShortConnectionEnable(final boolean clientShortConnectionEnable) {
        this.clientShortConnectionEnable = clientShortConnectionEnable;
    }

    public long getClientPublishServiceTimeout() {
        return clientPublishServiceTimeout;
    }

    public void setClientPublishServiceTimeout(final long clientPublishServiceTimeout) {
        this.clientPublishServiceTimeout = clientPublishServiceTimeout;
    }

    public long getClientConsumerServiceTimeout() {
        return clientConsumerServiceTimeout;
    }

    public void setClientConsumerServiceTimeout(final long clientConsumerServiceTimeout) {
        this.clientConsumerServiceTimeout = clientConsumerServiceTimeout;
    }

    public long getClientInvokeServiceTimeout() {
        return clientInvokeServiceTimeout;
    }

    public void setClientInvokeServiceTimeout(final long clientInvokeServiceTimeout) {
        this.clientInvokeServiceTimeout = clientInvokeServiceTimeout;
    }

    public int getClientMaxRetryCount() {
        return clientMaxRetryCount;
    }

    public void setClientMaxRetryCount(final int clientMaxRetryCount) {
        this.clientMaxRetryCount = clientMaxRetryCount;
    }

    public int getClientSleepBeforeRetry() {
        return clientSleepBeforeRetry;
    }

    public void setClientSleepBeforeRetry(final int clientSleepBeforeRetry) {
        this.clientSleepBeforeRetry = clientSleepBeforeRetry;
    }

    public int getServerListenPort() {
        return serverListenPort;
    }

    public void setServerListenPort(final int serverListenPort) {
        this.serverListenPort = serverListenPort;
    }

    public int getServerAcceptorThreads() {
        return serverAcceptorThreads;
    }

    public void setServerAcceptorThreads(final int serverAcceptorThreads) {
        this.serverAcceptorThreads = serverAcceptorThreads;
    }

    public int getServerIoThreads() {
        return serverIoThreads;
    }

    public void setServerIoThreads(final int serverIoThreads) {
        this.serverIoThreads = serverIoThreads;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(final int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerOnewayInvokeSemaphore() {
        return serverOnewayInvokeSemaphore;
    }

    public void setServerOnewayInvokeSemaphore(final int serverOnewayInvokeSemaphore) {
        this.serverOnewayInvokeSemaphore = serverOnewayInvokeSemaphore;
    }

    public int getServerAsyncInvokeSemaphore() {
        return serverAsyncInvokeSemaphore;
    }

    public void setServerAsyncInvokeSemaphore(final int serverAsyncInvokeSemaphore) {
        this.serverAsyncInvokeSemaphore = serverAsyncInvokeSemaphore;
    }

    public boolean isServerNativeEpollEnable() {
        return serverNativeEpollEnable;
    }

    public void setServerNativeEpollEnable(final boolean serverNativeEpollEnable) {
        this.serverNativeEpollEnable = serverNativeEpollEnable;
    }

    public int getServerAsyncCallbackExecutorThreads() {
        return serverAsyncCallbackExecutorThreads;
    }

    public void setServerAsyncCallbackExecutorThreads(final int serverAsyncCallbackExecutorThreads) {
        this.serverAsyncCallbackExecutorThreads = serverAsyncCallbackExecutorThreads;
    }

    public boolean isServerPooledBytebufAllocatorEnable() {
        return serverPooledBytebufAllocatorEnable;
    }

    public void setServerPooledBytebufAllocatorEnable(final boolean serverPooledBytebufAllocatorEnable) {
        this.serverPooledBytebufAllocatorEnable = serverPooledBytebufAllocatorEnable;
    }

    public boolean isServerAuthOpenEnable() {
        return serverAuthOpenEnable;
    }

    public void setServerAuthOpenEnable(final boolean serverAuthOpenEnable) {
        this.serverAuthOpenEnable = serverAuthOpenEnable;
    }
}
