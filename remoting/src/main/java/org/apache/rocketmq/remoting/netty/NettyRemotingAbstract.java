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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingRuntimeException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.interceptor.InterceptorInvoker;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.util.ThreadUtils;

public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    protected Semaphore semaphoreOneway;

    /**
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests.
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
        new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    protected final HashMap<Integer/* request code */, Pair<RequestProcessor, ExecutorService>> processorTable =
        new HashMap<Integer, Pair<RequestProcessor, ExecutorService>>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request
     * code.
     */
    protected Pair<RequestProcessor, ExecutorService> defaultRequestProcessor;

    /**
     * Used for async execute task for aysncInvokeMethod
     */
    private ExecutorService asyncExecuteService = ThreadUtils.newFixedThreadPool(5, 10000, "asyncExecute", false);

    /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    static {
        NettyLogger.initNettyLogger();
    }

    protected ScheduledExecutorService houseKeepingService = ThreadUtils.newSingleThreadScheduledExecutor("HouseKeepingService", true);

    public NettyRemotingAbstract() {
        this.semaphoreOneway = new Semaphore(65535, true);
        this.semaphoreAsync = new Semaphore(65535, true);
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    public void init(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * Entry of incoming command processing.
     *
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     *
     * @param ctx Channel handler context.
     * @param command incoming remoting command.
     * @throws Exception if there were any error while processing the incoming command.
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand command) throws Exception {
        final RemotingChannel remotingChannel = new NettyChannelHandlerContextImpl(ctx);
        if (command != null) {
            switch (command.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(remotingChannel, command);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(remotingChannel, command);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Process incoming request command issued by remote peer.
     *
     * @param remotingChannel channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final RemotingChannel remotingChannel, final RemotingCommand cmd) {
        NettyChannelHandlerContextImpl nettyChannel = (NettyChannelHandlerContextImpl) remotingChannel;
        final ChannelHandlerContext ctx = nettyChannel.getChannelHandlerContext();
        final Pair<RequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<RequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        final int opaque = cmd.getOpaque();
        final InterceptorGroup interceptorGroup = NettyRemotingAbstract.this.getInterceptorGroup();
        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        InterceptorInvoker.invokeBeforeRequest(interceptorGroup, remotingChannel, cmd);
                        final RemotingCommand response = pair.getObject1().processRequest(remotingChannel, cmd);
                        InterceptorInvoker.invokeAfterRequest(interceptorGroup, remotingChannel, cmd, response);

                        if (!cmd.isOnewayRPC()) {
                            if (response != null) {
                                response.setOpaque(opaque);
                                response.markResponseType();
                                try {
                                    ctx.writeAndFlush(response);
                                } catch (Throwable e) {
                                    log.error("process request over, but response failed", e);
                                    log.error(cmd.toString());
                                    log.error(response.toString());
                                }
                            }
                        }
                    } catch (Throwable throwable) {
                        log.error("Process request exception", throwable);
                        log.error(cmd.toString());
                        InterceptorInvoker.invokeOnException(interceptorGroup, remotingChannel, cmd, throwable, null);
                        int responseCode = RemotingSysResponseCode.SYSTEM_ERROR;
                        String responseMessage = RemotingHelper.exceptionSimpleDesc(throwable);
                        if (!cmd.isOnewayRPC()) {
                            if (throwable instanceof RemotingRuntimeException) {
                                RemotingRuntimeException remotingRuntimeException = (RemotingRuntimeException) throwable;
                                responseCode = remotingRuntimeException.getResponseCode();
                                responseMessage = remotingRuntimeException.getResponseMessage();
                            }
                            final RemotingCommand response = RemotingCommand.createResponseCommand(responseCode,
                                responseMessage);
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            if (pair.getObject1().rejectRequest()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[REJECTREQUEST]system busy, start flow control for a while");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                return;
            }

            try {
                final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException e) {
                if ((System.currentTimeMillis() % 10000) == 0) {
                    log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                        + ", too many requests and system thread pool busy, RejectedExecutionException "
                        + pair.getObject2().toString()
                        + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     * Process response from remote peer to the previous issued requests.
     *
     * @param remotingChannel remotingChannel.
     * @param cmd response command instance.
     */
    public void processResponseCommand(final RemotingChannel remotingChannel, RemotingCommand cmd) {
        final int opaque = cmd.getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            responseTable.remove(opaque);

            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            NettyChannelHandlerContextImpl nettyChannelHandlerContext = (NettyChannelHandlerContextImpl) remotingChannel;
            final ChannelHandlerContext ctx = nettyChannelHandlerContext.getChannelHandlerContext();
            log.warn("receive response, but not matched any request: {}, cmd: {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * Custom interceptor hook.
     *
     * @return RPC hook if specified; null otherwise.
     */
    public abstract InterceptorGroup getInterceptorGroup();

    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    protected void startUpHouseKeepingService() {
        this.houseKeepingService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanResponseTable();
            }
        }, 3000, 1000, TimeUnit.MICROSECONDS);
    }

    /**
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    public void start() {
        if (getChannelEventListener() != null) {
            nettyEventExecutor.start();
        }
    }

    public void shutdown() {
        if (this.nettyEventExecutor != null) {
            this.nettyEventExecutor.shutdown();
        }
        if (this.houseKeepingService != null) {
            this.houseKeepingService.shutdown();
        }
    }

    public RemotingCommand invokeSyncWithInterceptor(final RemotingChannel remotingChannel,
        final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        InterceptorGroup interceptorGroup = getInterceptorGroup();
        InterceptorInvoker.invokeBeforeRequest(interceptorGroup, remotingChannel, request);
        Channel channel = null;
        if (remotingChannel instanceof NettyChannelImpl) {
            channel = ((NettyChannelImpl) remotingChannel).getChannel();
        }
        try {
            RemotingCommand response = invokeSyncImpl(channel, request, timeoutMillis);
            InterceptorInvoker.invokeAfterRequest(interceptorGroup, remotingChannel, request, response);
            return response;
        } catch (InterruptedException | RemotingSendRequestException | RemotingTimeoutException ex) {
            InterceptorInvoker.invokeOnException(interceptorGroup, remotingChannel, request, ex, null);
            log.error("Sync invoke error", ex);
            throw ex;
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        final int opaque = request.getOpaque();

        try {
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            this.responseTable.put(opaque, responseFuture);
            final SocketAddress addr = channel.remoteAddress();
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(opaque);
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    log.warn("Send a request command to channel <" + addr + "> failed.");
                }
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);

            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                        responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }

            return responseCommand;
        } finally {
            this.responseTable.remove(opaque);
        }
    }

    abstract protected RemotingChannel getAndCreateChannel(final String addr, long timeout) throws InterruptedException;

    public void invokeAsyncWithInterceptor(
        final RemotingChannel remotingChannel,
        final RemotingCommand request,
        final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        InterceptorGroup interceptorGroup = this.getInterceptorGroup();
        InterceptorInvoker.invokeBeforeRequest(interceptorGroup, remotingChannel, request);
        Channel channel = null;
        if (remotingChannel instanceof NettyChannelImpl) {
            channel = ((NettyChannelImpl) remotingChannel).getChannel();
        }
        try {
            invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
        } catch (InterruptedException | RemotingTooMuchRequestException | RemotingTimeoutException | RemotingSendRequestException ex) {
            InterceptorInvoker.invokeOnException(interceptorGroup, remotingChannel, request, ex, null);
            throw ex;
        }
    }

    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis,
        final InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        invokeAsyncImpl(null, channel, request, timeoutMillis, invokeCallback);
    }

    public void invokeAsyncImpl(final String addr, final Channel currentChannel, final RemotingCommand request,
        final long timeoutMillis,
        final InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException {
        final long beginStartTime = System.currentTimeMillis();
        boolean acquired = semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(semaphoreAsync);
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("InvokeAsyncImpl call timeout");
            }
            final int opaque = request.getOpaque();
            final ResponseFuture responseFuture = new ResponseFuture(currentChannel, opaque, timeoutMillis, invokeCallback, once);
            responseTable.put(opaque, responseFuture);
            asyncExecuteService.submit(new Runnable() {
                @Override
                public void run() {
                    Channel channel = currentChannel;
                    final String remotingAddr = RemotingHelper.parseChannelRemoteAddr(channel);
                    try {
                        if (channel == null) {
                            RemotingChannel remotingChannel = getAndCreateChannel(addr, timeoutMillis);
                            if (remotingChannel != null && remotingChannel instanceof NettyChannelImpl) {
                                channel = ((NettyChannelImpl) remotingChannel).getChannel();
                            }
                            responseFuture.setProcessChannel(channel);
                        }
                        channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture f) throws Exception {
                                if (f.isSuccess()) {
                                    responseFuture.setSendRequestOK(true);
                                    return;
                                }
                                requestFail(opaque);
                                log.warn("send a request command to channel <{}> failed.", remotingAddr);
                            }
                        });
                    } catch (Exception ex) {
                        responseFuture.release();
                        requestFail(opaque);
                        log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", ex);
                    }
                }
            });
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                    String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreAsync.getQueueLength(),
                        this.semaphoreAsync.availablePermits()
                    );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     *
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> entry = it.next();
            if (entry.getValue().getProcessChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    public void invokeOnewayWithInterceptor(final RemotingChannel remotingChannel, final RemotingCommand request,
        final long timeoutMillis)
        throws
        InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        Channel channel = null;

        InterceptorGroup interceptorGroup = this.getInterceptorGroup();
        InterceptorInvoker.invokeBeforeRequest(interceptorGroup, remotingChannel, request);

        if (remotingChannel instanceof NettyChannelImpl) {
            channel = ((NettyChannelImpl) remotingChannel).getChannel();
        }
        try {
            invokeOnewayImpl(channel, request, timeoutMillis);
        } catch (InterruptedException | RemotingTooMuchRequestException | RemotingTimeoutException | RemotingSendRequestException ex) {
            InterceptorInvoker.invokeOnException(interceptorGroup, remotingChannel, request, ex, null);
            throw ex;
        }
    }

    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws
        InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                    timeoutMillis,
                    this.semaphoreOneway.getQueueLength(),
                    this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), new NettyChannelImpl(event.getChannel()));
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), new NettyChannelImpl(event.getChannel()));
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), new NettyChannelImpl(event.getChannel()));
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), new NettyChannelImpl(event.getChannel()));
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }

    public void registerNettyProcessor(int requestCode, RequestProcessor processor, ExecutorService executor) {
        Pair<RequestProcessor, ExecutorService> pair = new Pair<>(processor, executor);
        this.processorTable.put(requestCode, pair);
    }

    public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

}
