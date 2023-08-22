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

package org.apache.rocketmq.proxy.remoting;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.thread.ThreadPoolStatusMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.activity.AckMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.ChangeInvisibleTimeActivity;
import org.apache.rocketmq.proxy.remoting.activity.ClientManagerActivity;
import org.apache.rocketmq.proxy.remoting.activity.ConsumerManagerActivity;
import org.apache.rocketmq.proxy.remoting.activity.GetTopicRouteActivity;
import org.apache.rocketmq.proxy.remoting.activity.PopMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.PullMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.SendMessageActivity;
import org.apache.rocketmq.proxy.remoting.activity.TransactionActivity;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannelManager;
import org.apache.rocketmq.proxy.remoting.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class RemotingProtocolServer implements StartAndShutdown, RemotingProxyOutClient {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected final MessagingProcessor messagingProcessor;
    protected final RemotingChannelManager remotingChannelManager;
    protected final ChannelEventListener clientHousekeepingService;
    protected final RemotingServer defaultRemotingServer;
    protected final GetTopicRouteActivity getTopicRouteActivity;
    protected final ClientManagerActivity clientManagerActivity;
    protected final ConsumerManagerActivity consumerManagerActivity;
    protected final SendMessageActivity sendMessageActivity;
    protected final TransactionActivity transactionActivity;
    protected final PullMessageActivity pullMessageActivity;
    protected final PopMessageActivity popMessageActivity;
    protected final AckMessageActivity ackMessageActivity;
    protected final ChangeInvisibleTimeActivity changeInvisibleTimeActivity;
    protected final ThreadPoolExecutor sendMessageExecutor;
    protected final ThreadPoolExecutor pullMessageExecutor;
    protected final ThreadPoolExecutor heartbeatExecutor;
    protected final ThreadPoolExecutor updateOffsetExecutor;
    protected final ThreadPoolExecutor topicRouteExecutor;
    protected final ThreadPoolExecutor defaultExecutor;
    protected final ScheduledExecutorService timerExecutor;

    public RemotingProtocolServer(MessagingProcessor messagingProcessor, List<AccessValidator> accessValidators) {
        this.messagingProcessor = messagingProcessor;
        this.remotingChannelManager = new RemotingChannelManager(this, messagingProcessor.getProxyRelayService());

        RequestPipeline pipeline = createRequestPipeline(accessValidators);
        this.getTopicRouteActivity = new GetTopicRouteActivity(pipeline, messagingProcessor);
        this.clientManagerActivity = new ClientManagerActivity(pipeline, messagingProcessor, remotingChannelManager);
        this.consumerManagerActivity = new ConsumerManagerActivity(pipeline, messagingProcessor);
        this.sendMessageActivity = new SendMessageActivity(pipeline, messagingProcessor);
        this.transactionActivity = new TransactionActivity(pipeline, messagingProcessor);
        this.pullMessageActivity = new PullMessageActivity(pipeline, messagingProcessor);
        this.popMessageActivity = new PopMessageActivity(pipeline, messagingProcessor);
        this.ackMessageActivity = new AckMessageActivity(pipeline, messagingProcessor);
        this.changeInvisibleTimeActivity = new ChangeInvisibleTimeActivity(pipeline, messagingProcessor);

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        NettyServerConfig defaultServerConfig = new NettyServerConfig();
        defaultServerConfig.setListenPort(config.getRemotingListenPort());
        TlsSystemConfig.tlsTestModeEnable = config.isTlsTestModeEnable();
        System.setProperty(TlsSystemConfig.TLS_TEST_MODE_ENABLE, Boolean.toString(config.isTlsTestModeEnable()));
        TlsSystemConfig.tlsServerCertPath = config.getTlsCertPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_CERTPATH, config.getTlsCertPath());
        TlsSystemConfig.tlsServerKeyPath = config.getTlsKeyPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_KEYPATH, config.getTlsKeyPath());

        this.clientHousekeepingService = new ClientHousekeepingService(this.clientManagerActivity);

        if (config.isEnableRemotingLocalProxyGrpc()) {
            this.defaultRemotingServer = new MultiProtocolRemotingServer(defaultServerConfig, this.clientHousekeepingService);
        } else {
            this.defaultRemotingServer = new NettyRemotingServer(defaultServerConfig, this.clientHousekeepingService);
        }
        this.registerRemotingServer(this.defaultRemotingServer);

        this.sendMessageExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingSendMessageThreadPoolNums(),
            config.getRemotingSendMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingSendMessageThread",
            config.getRemotingSendThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInSendQueue())
        );

        this.pullMessageExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingPullMessageThreadPoolNums(),
            config.getRemotingPullMessageThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingPullMessageThread",
            config.getRemotingPullThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInPullQueue())
        );

        this.updateOffsetExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingUpdateOffsetThreadPoolNums(),
            config.getRemotingUpdateOffsetThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "RemotingUpdateOffsetThread",
            config.getRemotingUpdateOffsetThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInUpdateOffsetQueue())
        );

        this.heartbeatExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingHeartbeatThreadPoolNums(),
            config.getRemotingHeartbeatThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingHeartbeatThread",
            config.getRemotingHeartbeatThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInHeartbeatQueue())
        );

        this.topicRouteExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingTopicRouteThreadPoolNums(),
            config.getRemotingTopicRouteThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingTopicRouteThread",
            config.getRemotingTopicRouteThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInTopicRouteQueue())
        );

        this.defaultExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getRemotingDefaultThreadPoolNums(),
            config.getRemotingDefaultThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "RemotingDefaultThread",
            config.getRemotingDefaultThreadPoolQueueCapacity(),
            new ThreadPoolHeadSlowTimeMillsMonitor(config.getRemotingWaitTimeMillsInDefaultQueue())
        );

        this.timerExecutor = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("RemotingServerScheduler-%d").build()
        );
        this.timerExecutor.scheduleAtFixedRate(this::cleanExpireRequest, 10, 10, TimeUnit.SECONDS);
    }

    protected void registerRemotingServer(RemotingServer remotingServer) {
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageActivity, this.sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageActivity, sendMessageExecutor);

        remotingServer.registerProcessor(RequestCode.END_TRANSACTION, transactionActivity, sendMessageExecutor);

        remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManagerActivity, this.heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientManagerActivity, this.defaultExecutor);

        remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, pullMessageActivity, this.pullMessageExecutor);
        remotingServer.registerProcessor(RequestCode.LITE_PULL_MESSAGE, pullMessageActivity, this.pullMessageExecutor);
        remotingServer.registerProcessor(RequestCode.POP_MESSAGE, pullMessageActivity, this.pullMessageExecutor);

        remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.ACK_MESSAGE, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, consumerManagerActivity, this.updateOffsetExecutor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_CONNECTION_LIST, consumerManagerActivity, this.updateOffsetExecutor);

        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.GET_MAX_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.GET_MIN_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.LOCK_BATCH_MQ, consumerManagerActivity, this.defaultExecutor);
        remotingServer.registerProcessor(RequestCode.UNLOCK_BATCH_MQ, consumerManagerActivity, this.defaultExecutor);

        remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, getTopicRouteActivity, this.topicRouteExecutor);
    }

    @Override
    public void shutdown() throws Exception {
        this.defaultRemotingServer.shutdown();
        this.remotingChannelManager.shutdown();
        this.sendMessageExecutor.shutdown();
        this.pullMessageExecutor.shutdown();
        this.heartbeatExecutor.shutdown();
        this.updateOffsetExecutor.shutdown();
        this.topicRouteExecutor.shutdown();
        this.defaultExecutor.shutdown();
    }

    @Override
    public void start() throws Exception {
        this.remotingChannelManager.start();
        this.defaultRemotingServer.start();
    }

    @Override
    public CompletableFuture<RemotingCommand> invokeToClient(Channel channel, RemotingCommand request,
        long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            this.defaultRemotingServer.invokeAsync(channel, request, timeoutMillis, new InvokeCallback() {
                @Override
                public void operationSuccess(RemotingCommand response) {
                    future.complete(response);
                }

                @Override
                public void operationException(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected RequestPipeline createRequestPipeline(List<AccessValidator> accessValidators) {
        RequestPipeline pipeline = (ctx, request, context) -> {
        };
        // add pipeline
        // the last pipe add will execute at the first
        return pipeline.pipe(new AuthenticationPipeline(accessValidators));
    }

    protected class ThreadPoolHeadSlowTimeMillsMonitor implements ThreadPoolStatusMonitor {

        private final long maxWaitTimeMillsInQueue;

        public ThreadPoolHeadSlowTimeMillsMonitor(long maxWaitTimeMillsInQueue) {
            this.maxWaitTimeMillsInQueue = maxWaitTimeMillsInQueue;
        }

        @Override
        public String describe() {
            return "headSlow";
        }

        @Override
        public double value(ThreadPoolExecutor executor) {
            return headSlowTimeMills(executor.getQueue());
        }

        @Override
        public boolean needPrintJstack(ThreadPoolExecutor executor, double value) {
            return value > maxWaitTimeMillsInQueue;
        }
    }

    protected long headSlowTimeMills(BlockingQueue<Runnable> q) {
        try {
            long slowTimeMills = 0;
            final Runnable peek = q.peek();
            if (peek != null) {
                RequestTask rt = castRunnable(peek);
                slowTimeMills = rt == null ? 0 : System.currentTimeMillis() - rt.getCreateTimestamp();
            }

            if (slowTimeMills < 0) {
                slowTimeMills = 0;
            }

            return slowTimeMills;
        } catch (Exception e) {
            log.error("error when headSlowTimeMills.", e);
        }
        return -1;
    }

    protected void cleanExpireRequest() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        cleanExpiredRequestInQueue(this.sendMessageExecutor, config.getRemotingWaitTimeMillsInSendQueue());
        cleanExpiredRequestInQueue(this.pullMessageExecutor, config.getRemotingWaitTimeMillsInPullQueue());
        cleanExpiredRequestInQueue(this.heartbeatExecutor, config.getRemotingWaitTimeMillsInHeartbeatQueue());
        cleanExpiredRequestInQueue(this.updateOffsetExecutor, config.getRemotingWaitTimeMillsInUpdateOffsetQueue());
        cleanExpiredRequestInQueue(this.topicRouteExecutor, config.getRemotingWaitTimeMillsInTopicRouteQueue());
        cleanExpiredRequestInQueue(this.defaultExecutor, config.getRemotingWaitTimeMillsInDefaultQueue());
    }

    protected void cleanExpiredRequestInQueue(ThreadPoolExecutor threadPoolExecutor, long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
                BlockingQueue<Runnable> blockingQueue = threadPoolExecutor.getQueue();
                if (!blockingQueue.isEmpty()) {
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = castRunnable(runnable);
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= maxWaitTimeMillsInQueue) {
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            rt.returnResponse(ResponseCode.SYSTEM_BUSY,
                                String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    private RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt futureTaskExt = (FutureTaskExt) runnable;
                return (RequestTask) futureTaskExt.getRunnable();
            }
            return null;
        } catch (Throwable e) {
            log.error("castRunnable exception. class:{}", runnable.getClass().getName(), e);
        }

        return null;
    }
}
