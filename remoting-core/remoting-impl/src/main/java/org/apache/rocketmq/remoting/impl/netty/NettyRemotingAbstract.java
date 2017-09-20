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

package org.apache.rocketmq.remoting.impl.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingEndPoint;
import org.apache.rocketmq.remoting.api.RemotingService;
import org.apache.rocketmq.remoting.api.RequestProcessor;
import org.apache.rocketmq.remoting.api.channel.ChannelEventListener;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.RemotingCommandFactory;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.api.exception.RemoteTimeoutException;
import org.apache.rocketmq.remoting.api.interceptor.ExceptionContext;
import org.apache.rocketmq.remoting.api.interceptor.Interceptor;
import org.apache.rocketmq.remoting.api.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.api.interceptor.RequestContext;
import org.apache.rocketmq.remoting.api.interceptor.ResponseContext;
import org.apache.rocketmq.remoting.api.protocol.ProtocolFactory;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.api.serializable.SerializerFactory;
import org.apache.rocketmq.remoting.common.ChannelEventListenerGroup;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingCommandFactoryMeta;
import org.apache.rocketmq.remoting.common.ResponseResult;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.channel.NettyChannelImpl;
import org.apache.rocketmq.remoting.impl.command.RemotingCommandFactoryImpl;
import org.apache.rocketmq.remoting.impl.protocol.ProtocolFactoryImpl;
import org.apache.rocketmq.remoting.impl.protocol.serializer.SerializerFactoryImpl;
import org.apache.rocketmq.remoting.internal.UIDGenerator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NettyRemotingAbstract implements RemotingService {
    protected static final Logger LOG = LoggerFactory.getLogger(NettyRemotingAbstract.class);
    protected final ProtocolFactory protocolFactory = new ProtocolFactoryImpl();
    protected final SerializerFactory serializerFactory = new SerializerFactoryImpl();
    protected final ChannelEventExecutor channelEventExecutor = new ChannelEventExecutor("ChannelEventExecutor");
    private final Semaphore semaphoreOneway;
    private final Semaphore semaphoreAsync;
    private final Map<Integer, ResponseResult> ackTables = new ConcurrentHashMap<Integer, ResponseResult>(256);
    private final Map<String, Pair<RequestProcessor, ExecutorService>> processorTables = new ConcurrentHashMap<String, Pair<RequestProcessor, ExecutorService>>();
    private final AtomicLong responseCounter = new AtomicLong(0);
    private final RemotingCommandFactory remotingCommandFactory;
    private final String remotingInstanceId = UIDGenerator.instance().createUID();

    private final ExecutorService publicExecutor;
    protected ScheduledExecutorService houseKeepingService = ThreadUtils.newSingleThreadScheduledExecutor("HouseKeepingService", true);
    private InterceptorGroup interceptorGroup = new InterceptorGroup();
    private ChannelEventListenerGroup channelEventListenerGroup = new ChannelEventListenerGroup();

    NettyRemotingAbstract(RemotingConfig clientConfig) {
        this(clientConfig, new RemotingCommandFactoryMeta());
    }

    NettyRemotingAbstract(RemotingConfig clientConfig, RemotingCommandFactoryMeta remotingCommandFactoryMeta) {
        this.semaphoreOneway = new Semaphore(clientConfig.getClientOnewayInvokeSemaphore(), true);
        this.semaphoreAsync = new Semaphore(clientConfig.getClientAsyncInvokeSemaphore(), true);
        this.publicExecutor = ThreadUtils.newThreadPoolExecutor(
            clientConfig.getClientAsyncCallbackExecutorThreads(),
            clientConfig.getClientAsyncCallbackExecutorThreads(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(10000),
            "PublicExecutor", true);
        this.remotingCommandFactory = new RemotingCommandFactoryImpl(remotingCommandFactoryMeta);
    }

    public SerializerFactory getSerializerFactory() {
        return serializerFactory;
    }

    protected void putNettyEvent(final NettyChannelEvent event) {
        this.channelEventExecutor.putNettyEvent(event);
    }

    protected void startUpHouseKeepingService() {
        this.houseKeepingService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanResponseTable();
            }
        }, 3000, 1000, TimeUnit.MICROSECONDS);
    }

    @Override
    public void start() {
        if (this.channelEventListenerGroup.size() > 0) {
            this.channelEventExecutor.start();
        }
    }

    @Override
    public void stop() {
        ThreadUtils.shutdownGracefully(publicExecutor, 2000, TimeUnit.MILLISECONDS);
        ThreadUtils.shutdownGracefully(channelEventExecutor);
    }

    protected void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand command) throws Exception {
        if (command != null) {
            switch (command.trafficType()) {
                case REQUEST_ONEWAY:
                case REQUEST_ASYNC:
                case REQUEST_SYNC:
                    processRequestCommand(ctx, command);
                    break;
                case RESPONSE:
                    processResponseCommand(ctx, command);
                    break;
                default:
                    LOG.warn("Not supported The traffic type {} !", command.trafficType());
                    break;
            }
        }
    }

    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        Pair<RequestProcessor, ExecutorService> processorExecutorPair = this.processorTables.get(cmd.opCode());

        RemotingChannel channel = new NettyChannelImpl(ctx.channel());

        Runnable run = buildProcessorTask(ctx, cmd, processorExecutorPair, channel);

        try {
            processorExecutorPair.getRight().submit(run);
        } catch (RejectedExecutionException e) {
            if ((System.currentTimeMillis() % 10000) == 0) {
                LOG.warn(String.format("Request %s from %s Rejected by server executor %s !", cmd,
                    extractRemoteAddress(ctx.channel()), processorExecutorPair.getRight().toString()));
            }

            if (cmd.trafficType() != TrafficType.REQUEST_ONEWAY) {
                interceptorGroup.onException(new ExceptionContext(RemotingEndPoint.RESPONSE,
                    extractRemoteAddress(ctx.channel()), cmd, e, "FLOW_CONTROL"));

                RemotingCommand response = remotingCommandFactory.createResponse(cmd);
                response.opCode(RemotingCommand.CommandFlag.ERROR.flag());
                response.remark("SYSTEM_BUSY");
                writeAndFlush(ctx.channel(), response);
            }
        }
    }

    @NotNull
    private Runnable buildProcessorTask(final ChannelHandlerContext ctx, final RemotingCommand cmd,
        final Pair<RequestProcessor, ExecutorService> processorExecutorPair, final RemotingChannel channel) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.RESPONSE,
                        extractRemoteAddress(ctx.channel()), cmd));

                    RemotingCommand response = processorExecutorPair.getLeft().processRequest(channel, cmd);

                    interceptorGroup.afterResponseReceived(new ResponseContext(RemotingEndPoint.RESPONSE,
                        extractRemoteAddress(ctx.channel()), cmd, response));

                    handleResponse(response, cmd, ctx);
                } catch (Throwable e) {
                    LOG.error(String.format("Process request %s error !", cmd.toString()), e);

                    handleException(e, cmd, ctx);
                }
            }
        };
    }

    private void handleException(Throwable e, RemotingCommand cmd, ChannelHandlerContext ctx) {
        if (cmd.trafficType() != TrafficType.REQUEST_ONEWAY) {
            //FiXME Exception interceptor can not throw exception
            interceptorGroup.onException(new ExceptionContext(RemotingEndPoint.RESPONSE, extractRemoteAddress(ctx.channel()), cmd, e, ""));
            RemotingCommand response = remotingCommandFactory.createResponse(cmd);
            response.opCode(RemotingCommand.CommandFlag.ERROR.flag());
            response.remark(serializeException(cmd.serializerType(), e));
            response.property("Exception", e.getClass().getName());
            ctx.writeAndFlush(response);
        }
    }

    private String serializeException(byte serializeType, Throwable exception) {
        final Serializer serialization = getSerializerFactory().get(serializeType);
        return serialization.encode(exception).toString();
    }

    private void handleResponse(RemotingCommand response, RemotingCommand cmd, ChannelHandlerContext ctx) {
        if (cmd.trafficType() != TrafficType.REQUEST_ONEWAY) {
            if (response != null) {
                try {
                    writeAndFlush(ctx.channel(), response);
                } catch (Throwable e) {
                    LOG.error(String.format("Process request %s success, but transfer response %s failed !",
                        cmd.toString(), response.toString()), e);
                }
            }
        }

    }

    private void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final ResponseResult responseResult = ackTables.get(cmd.requestID());
        if (responseResult != null) {
            responseResult.setResponseCommand(cmd);
            responseResult.release();

            long time = System.currentTimeMillis();
            ackTables.remove(cmd.requestID());
            if (responseCounter.incrementAndGet() % 5000 == 0) {
                LOG.info("REQUEST ID:{}, cost time:{}, ackTables.size:{}", cmd.requestID(), time - responseResult.getBeginTimestamp(),
                    ackTables.size());
            }
            if (responseResult.getAsyncHandler() != null) {
                boolean sameThread = false;
                ExecutorService executor = this.getCallbackExecutor();
                if (executor != null) {
                    try {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    responseResult.executeCallbackArrived(responseResult.getResponseCommand());
                                } catch (Throwable e) {
                                    LOG.warn("Execute callback error !", e);
                                }
                            }
                        });
                    } catch (RejectedExecutionException e) {
                        sameThread = true;
                        LOG.warn("Execute submit error !", e);
                    }
                } else {
                    sameThread = true;
                }

                if (sameThread) {
                    try {
                        responseResult.executeCallbackArrived(responseResult.getResponseCommand());
                    } catch (Throwable e) {
                        LOG.warn("Execute callback in response thread error !", e);
                    }
                }
            } else {
                responseResult.putResponse(cmd);
            }
        } else {
            LOG.warn("request {} from {} has not matched response !", cmd, extractRemoteAddress(ctx.channel()));
        }
    }

    private void writeAndFlush(final Channel channel, final Object msg, final ChannelFutureListener listener) {
        channel.writeAndFlush(msg).addListener(listener);
    }

    private void writeAndFlush(final Channel channel, final Object msg) {
        channel.writeAndFlush(msg);
    }

    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    void scanResponseTable() {
        /*
        Iterator<Map.Entry<Integer, ResponseResult>> iterator = this.ackTables.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, ResponseResult> next = iterator.next();
            ResponseResult result = next.getValue();

            if ((result.getBeginTimestamp() + result.getTimeoutMillis()) <= System.currentTimeMillis()) {
                iterator.remove();
                try {
                    long timeoutMillis = result.getTimeoutMillis();
                    long costTimeMillis = System.currentTimeMillis() - result.getBeginTimestamp();
                    result.onTimeout(timeoutMillis, costTimeMillis);
                    LOG.error("scan response table command {} failed", result.getRequestId());
                } catch (Throwable e) {
                    LOG.warn("Error occurred when execute timeout callback !", e);
                } finally {
                    result.release();
                    LOG.warn("Removed timeout request {} ", result);
                }
            }
        }
        */
    }

    public RemotingCommand invokeWithInterceptor(final Channel channel, final RemotingCommand request,
        long timeoutMillis) {
        request.trafficType(TrafficType.REQUEST_SYNC);

        final String remoteAddr = extractRemoteAddress(channel);

        //FIXME try catch here
        this.interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.REQUEST, remoteAddr, request));

        RemotingCommand responseCommand = this.invoke0(remoteAddr, channel, request, timeoutMillis);

        this.interceptorGroup.afterResponseReceived(new ResponseContext(RemotingEndPoint.REQUEST,
            extractRemoteAddress(channel), request, responseCommand));

        return responseCommand;
    }

    private RemotingCommand invoke0(final String remoteAddr, final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        try {
            final int opaque = request.requestID();
            final ResponseResult responseResult = new ResponseResult(opaque, timeoutMillis);
            responseResult.setRequestCommand(request);
            //FIXME one interceptor for all case ?
            responseResult.setInterceptorGroup(this.interceptorGroup);
            responseResult.setRemoteAddr(remoteAddr);

            this.ackTables.put(opaque, responseResult);

            ChannelFutureListener listener = new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseResult.setSendRequestOK(true);
                        return;
                    } else {
                        responseResult.setSendRequestOK(false);

                        ackTables.remove(opaque);
                        responseResult.setCause(f.cause());
                        responseResult.putResponse(null);

                        LOG.warn("Send request command to {} failed !", remoteAddr);
                    }
                }
            };

            this.writeAndFlush(channel, request, listener);

            RemotingCommand responseCommand = responseResult.waitResponse(timeoutMillis);

            if (null == responseCommand) {
                if (responseResult.isSendRequestOK()) {
                    throw new RemoteTimeoutException(extractRemoteAddress(channel), timeoutMillis, responseResult.getCause());
                }
                /*
                else {
                    throw new RemoteAccessException(extractRemoteAddress(channel), responseResult.getCause());
                }*/
            }

            return responseCommand;
        } finally {
            this.ackTables.remove(request.requestID());
        }
    }

    public void invokeAsyncWithInterceptor(final Channel channel, final RemotingCommand request,
        final AsyncHandler invokeCallback, long timeoutMillis) {
        request.trafficType(TrafficType.REQUEST_ASYNC);

        final String remoteAddr = extractRemoteAddress(channel);

        this.interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.REQUEST, remoteAddr, request));

        Exception exception = null;

        try {
            this.invokeAsync0(remoteAddr, channel, request, timeoutMillis, invokeCallback);
        } catch (InterruptedException e) {
            exception = e;
        } finally {
            if (null != exception) {
                try {
                    this.interceptorGroup.onException(new ExceptionContext(RemotingEndPoint.REQUEST, extractRemoteAddress(channel), request, exception, "REMOTING_EXCEPTION"));
                } catch (Throwable e) {
                    LOG.warn("onException ", e);
                }
            }
        }
    }

    private void invokeAsync0(final String remoteAddr, final Channel channel, final RemotingCommand request,
        final long timeoutMillis, final AsyncHandler invokeCallback) throws InterruptedException {
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final int requestID = request.requestID();

            SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            final ResponseResult responseResult = new ResponseResult(request.requestID(), timeoutMillis, invokeCallback, once);
            responseResult.setRequestCommand(request);
            responseResult.setInterceptorGroup(this.interceptorGroup);
            responseResult.setRemoteAddr(remoteAddr);

            this.ackTables.put(request.requestID(), responseResult);
            try {
                ChannelFutureListener listener = new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        responseResult.setSendRequestOK(f.isSuccess());
                        if (f.isSuccess()) {
                            return;
                        }

                        responseResult.putResponse(null);
                        ackTables.remove(requestID);
                        try {
                            responseResult.executeRequestSendFailed();
                        } catch (Throwable e) {
                            LOG.warn("Execute callback error !", e);
                        } finally {
                            responseResult.release();
                        }

                        LOG.warn("Send request command to channel  failed.", remoteAddr);
                    }
                };

                this.writeAndFlush(channel, request, listener);
            } catch (Exception e) {
                responseResult.release();
                LOG.error("Send request command to channel " + channel + " error !", e);
            }
        } else {
            String info = String.format("Semaphore tryAcquire %d ms timeout for request %s ,waiting thread nums: %d,availablePermits: %d",
                timeoutMillis, request.toString(), semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
            LOG.error(info);
            throw new RemoteTimeoutException(info);
        }
    }

    public void invokeOnewayWithInterceptor(final Channel channel, final RemotingCommand request, long timeoutMillis) {
        request.trafficType(TrafficType.REQUEST_ONEWAY);

        this.interceptorGroup.beforeRequest(new RequestContext(RemotingEndPoint.REQUEST, extractRemoteAddress(channel), request));

        Exception exception = null;

        try {
            this.invokeOneway0(channel, request, timeoutMillis);
        } catch (InterruptedException e) {
            exception = e;
        } finally {
            if (null != exception) {
                try {
                    this.interceptorGroup.onException(new ExceptionContext(RemotingEndPoint.REQUEST, extractRemoteAddress(channel), request, exception, "REMOTING_EXCEPTION"));
                } catch (Throwable e) {
                    LOG.warn("onException ", e);
                }
            }
        }
    }

    private void invokeOneway0(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException {
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                final SocketAddress socketAddress = channel.remoteAddress();

                ChannelFutureListener listener = new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            LOG.warn("Send request command to channel {} failed !", socketAddress);
                        }
                    }
                };

                this.writeAndFlush(channel, request, listener);
            } catch (Exception e) {
                once.release();
                LOG.error("Send request command to channel " + channel + " error !", e);
            }
        } else {
            String info = String.format("Semaphore tryAcquire %d ms timeout for request %s ,waiting thread nums: %d,availablePermits: %d",
                timeoutMillis, request.toString(), semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
            LOG.error(info);
            throw new RemoteTimeoutException(info);
        }
    }

    public String getRemotingInstanceId() {
        return remotingInstanceId;
    }

    @Override
    public ProtocolFactory protocolFactory() {
        return this.protocolFactory;
    }

    @Override
    public SerializerFactory serializerFactory() {
        return this.serializerFactory;
    }

    @Override
    public RemotingCommandFactory commandFactory() {
        return this.remotingCommandFactory;
    }

    @Override
    public void registerRequestProcessor(String requestCode, RequestProcessor processor, ExecutorService executor) {
        Pair<RequestProcessor, ExecutorService> pair = new Pair<RequestProcessor, ExecutorService>(processor, executor);
        if (!this.processorTables.containsKey(requestCode)) {
            this.processorTables.put(requestCode, pair);
        }
    }

    @Override
    public void registerRequestProcessor(String requestCode, RequestProcessor processor) {
        this.registerRequestProcessor(requestCode, processor, publicExecutor);
    }

    @Override
    public void unregisterRequestProcessor(String requestCode) {
        this.processorTables.remove(requestCode);
    }

    @Override
    public String remotingInstanceId() {
        return this.getRemotingInstanceId();
    }

    @Override
    public void registerInterceptor(Interceptor interceptor) {
        this.interceptorGroup.registerInterceptor(interceptor);
    }

    @Override
    public void registerChannelEventListener(ChannelEventListener listener) {
        this.channelEventListenerGroup.registerChannelEventListener(listener);
    }

    @Override
    public Pair<RequestProcessor, ExecutorService> processor(String requestCode) {
        return processorTables.get(requestCode);
    }

    protected String extractRemoteAddress(Channel channel) {
        return ((InetSocketAddress) channel.remoteAddress()).getAddress().getHostAddress();
    }

    class ChannelEventExecutor extends Thread {
        private final static int MAX_SIZE = 10000;
        private final LinkedBlockingQueue<NettyChannelEvent> eventQueue = new LinkedBlockingQueue<NettyChannelEvent>();
        private String name;

        public ChannelEventExecutor(String nettyEventExector) {
            super(nettyEventExector);
            this.name = nettyEventExector;
        }
        //private final AtomicBoolean isStopped = new AtomicBoolean(true);

        public void putNettyEvent(final NettyChannelEvent event) {
            if (this.eventQueue.size() <= MAX_SIZE) {
                this.eventQueue.add(event);
            } else {
                LOG.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            LOG.info(this.name + " service started");

            ChannelEventListenerGroup listener = NettyRemotingAbstract.this.channelEventListenerGroup;

            while (true) {
                try {
                    NettyChannelEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        RemotingChannel channel = new NettyChannelImpl(event.getChannel());

                        LOG.warn("Channel Event, {}", event);

                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(channel);
                                break;
                            case INACTIVE:
                                listener.onChannelClose(channel);
                                break;
                            case ACTIVE:
                                listener.onChannelConnect(channel);
                                break;
                            case EXCEPTION:
                                listener.onChannelException(channel);
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    LOG.error("error", e);
                    break;
                }
            }
        }

    }

    protected class EventDispatcher extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

}
