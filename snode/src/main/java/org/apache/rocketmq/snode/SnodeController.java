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
package org.apache.rocketmq.snode;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingClientFactory;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.RemotingServerFactory;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.interceptor.ExceptionContext;
import org.apache.rocketmq.remoting.interceptor.Interceptor;
import org.apache.rocketmq.remoting.interceptor.InterceptorFactory;
import org.apache.rocketmq.remoting.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.interceptor.RequestContext;
import org.apache.rocketmq.remoting.interceptor.ResponseContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.util.ServiceProvider;
import org.apache.rocketmq.snode.client.ClientHousekeepingService;
import org.apache.rocketmq.snode.client.ClientManager;
import org.apache.rocketmq.snode.client.SlowConsumerService;
import org.apache.rocketmq.snode.client.SubscriptionGroupManager;
import org.apache.rocketmq.snode.client.SubscriptionManager;
import org.apache.rocketmq.snode.client.impl.ConsumerManagerImpl;
import org.apache.rocketmq.snode.client.impl.ProducerManagerImpl;
import org.apache.rocketmq.snode.client.impl.SlowConsumerServiceImpl;
import org.apache.rocketmq.snode.client.impl.SubscriptionManagerImpl;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.snode.offset.ConsumerOffsetManager;
import org.apache.rocketmq.snode.processor.ConsumerManageProcessor;
import org.apache.rocketmq.snode.processor.HeartbeatProcessor;
import org.apache.rocketmq.snode.processor.PullMessageProcessor;
import org.apache.rocketmq.snode.processor.SendMessageProcessor;
import org.apache.rocketmq.snode.service.ClientService;
import org.apache.rocketmq.snode.service.EnodeService;
import org.apache.rocketmq.snode.service.NnodeService;
import org.apache.rocketmq.snode.service.PushService;
import org.apache.rocketmq.snode.service.ScheduledService;
import org.apache.rocketmq.snode.service.impl.ClientServiceImpl;
import org.apache.rocketmq.snode.service.impl.EnodeServiceImpl;
import org.apache.rocketmq.snode.service.impl.NnodeServiceImpl;
import org.apache.rocketmq.snode.service.impl.PushServiceImpl;
import org.apache.rocketmq.snode.service.impl.ScheduledServiceImpl;

public class SnodeController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final SnodeConfig snodeConfig;
    private final ServerConfig nettyServerConfig;
    private final ClientConfig nettyClientConfig;
    private RemotingClient remotingClient;
    private RemotingServer snodeServer;
    private ExecutorService sendMessageExecutor;
    private ExecutorService heartbeatExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService consumerManageExecutor;
    private EnodeService enodeService;
    private NnodeService nnodeService;
    private ExecutorService consumerManagerExecutor;
    private ScheduledService scheduledService;
    private ClientManager producerManager;
    private ClientManager consumerManager;
    private SubscriptionManager subscriptionManager;
    private ClientHousekeepingService clientHousekeepingService;
    private SubscriptionGroupManager subscriptionGroupManager;
    private ConsumerOffsetManager consumerOffsetManager;
    private ConsumerManageProcessor consumerManageProcessor;
    private SendMessageProcessor sendMessageProcessor;
    private PullMessageProcessor pullMessageProcessor;
    private HeartbeatProcessor heartbeatProcessor;
    private InterceptorGroup remotingServerInterceptorGroup;
    private InterceptorGroup consumeMessageInterceptorGroup;
    private InterceptorGroup sendMessageInterceptorGroup;
    private PushService pushService;
    private ClientService clientService;
    private SlowConsumerService slowConsumerService;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "SnodeControllerScheduledThread"));

    public SnodeController(ServerConfig nettyServerConfig,
        ClientConfig nettyClientConfig,
        SnodeConfig snodeConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.snodeConfig = snodeConfig;
        this.enodeService = new EnodeServiceImpl(this);
        this.nnodeService = new NnodeServiceImpl(this);
        this.scheduledService = new ScheduledServiceImpl(this);
        this.remotingClient = RemotingClientFactory.getInstance().createRemotingClient().init(this.getNettyClientConfig(), null);

        this.sendMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeSendMessageMinPoolSize(),
            snodeConfig.getSnodeSendMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(snodeConfig.getSnodeSendThreadPoolQueueCapacity()),
            "SnodeSendMessageThread",
            false);

        this.pullMessageExecutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeSendMessageMinPoolSize(),
            snodeConfig.getSnodeSendMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(snodeConfig.getSnodeSendThreadPoolQueueCapacity()),
            "SnodePullMessageThread",
            false);

        this.heartbeatExecutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeHeartBeatCorePoolSize(),
            snodeConfig.getSnodeHeartBeatMaxPoolSize(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(snodeConfig.getSnodeHeartBeatThreadPoolQueueCapacity()),
            "SnodeHeartbeatThread",
            true);

        this.consumerManagerExecutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeSendMessageMinPoolSize(),
            snodeConfig.getSnodeSendMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(snodeConfig.getSnodeSendThreadPoolQueueCapacity()),
            "SnodePullMessageThread",
            false);

        this.consumerManageExecutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeSendMessageMinPoolSize(),
            snodeConfig.getSnodeSendMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(snodeConfig.getSnodeSendThreadPoolQueueCapacity()),
            "ConsumerManagerThread",
            false);

        if (this.snodeConfig.getNamesrvAddr() != null) {
            this.nnodeService.updateNnodeAddressList(this.snodeConfig.getNamesrvAddr());
            log.info("Set user specified name server address: {}", this.snodeConfig.getNamesrvAddr());
        }

//        this.producerManager = new ProducerManager();

//        ConsumerIdsChangeListener consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
//        this.consumerManager = new ConsumerManager(consumerIdsChangeListener);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.consumerManageProcessor = new ConsumerManageProcessor(this);
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.heartbeatProcessor = new HeartbeatProcessor(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pushService = new PushServiceImpl(this);
        this.clientService = new ClientServiceImpl(this);
        this.subscriptionManager = new SubscriptionManagerImpl();
        this.producerManager = new ProducerManagerImpl();
        this.consumerManager = new ConsumerManagerImpl(this);
        this.clientHousekeepingService = new ClientHousekeepingService(this.producerManager, this.consumerManager);
        this.slowConsumerService = new SlowConsumerServiceImpl(this);
    }

    public SnodeConfig getSnodeConfig() {
        return snodeConfig;
    }

    private void initRemotingServerInterceptorGroup() {
        List<Interceptor> remotingServerInterceptors = InterceptorFactory.getInstance().loadInterceptors(this.snodeConfig.getRemotingServerInterceptorPath());
        if (remotingServerInterceptors != null && remotingServerInterceptors.size() > 0) {
            if (this.remotingServerInterceptorGroup == null) {
                this.remotingServerInterceptorGroup = new InterceptorGroup();
            }
            for (Interceptor interceptor : remotingServerInterceptors) {
                this.remotingServerInterceptorGroup.registerInterceptor(interceptor);
                log.warn("Remoting server interceptor: {} registered!", interceptor.interceptorName());
            }
        }
    }

    public boolean initialize() {
        this.snodeServer = RemotingServerFactory.getInstance().createRemotingServer().init(this.nettyServerConfig, this.clientHousekeepingService);
        this.registerProcessor();
        initSnodeInterceptorGroup();
        initRemotingServerInterceptorGroup();
        initAclInterceptorGroup();
        this.snodeServer.registerInterceptorGroup(this.remotingServerInterceptorGroup);
        return true;
    }

    private void initAclInterceptorGroup() {

        if (!this.snodeConfig.isAclEnable()) {
            log.info("The snode dose not enable acl");
            return;
        }

        List<AccessValidator> accessValidators = ServiceProvider.loadServiceList(ServiceProvider.ACL_VALIDATOR_ID, AccessValidator.class);
        if (accessValidators == null || accessValidators.isEmpty()) {
            log.info("The snode dose not load the AccessValidator");
            return;
        }

        for (AccessValidator accessValidator: accessValidators) {
            final AccessValidator validator = accessValidator;
            this.remotingServerInterceptorGroup.registerInterceptor(new Interceptor() {

                @Override
                public String interceptorName() {
                    return "snodeRequestAclControlInterceptor";
                }

                @Override public void beforeRequest(RequestContext requestContext) {
                    //Do not catch the exception
                    RemotingCommand request = requestContext.getRequest();
                    String remoteAddr = RemotingUtil.socketAddress2IpString(requestContext.getRemotingChannel().remoteAddress());
                    validator.validate(validator.parse(request, remoteAddr));

                }

                @Override public void afterRequest(ResponseContext responseContext) { }

                @Override public void onException(ExceptionContext exceptionContext) { }
            });
        }
    }

    private void initSnodeInterceptorGroup() {
        List<Interceptor> consumeMessageInterceptors = InterceptorFactory.getInstance().loadInterceptors(this.snodeConfig.getConsumeMessageInterceptorPath());
        if (consumeMessageInterceptors != null && consumeMessageInterceptors.size() > 0) {
            this.consumeMessageInterceptorGroup = new InterceptorGroup();
            for (Interceptor interceptor : consumeMessageInterceptors) {
                this.consumeMessageInterceptorGroup.registerInterceptor(interceptor);
                log.warn("Consume message interceptor: {} registered!", interceptor.interceptorName());
            }
        }
        List<Interceptor> sendMessageInterceptors = InterceptorFactory.getInstance().loadInterceptors(this.snodeConfig.getSendMessageInterceptorPath());
        if (sendMessageInterceptors != null && sendMessageInterceptors.size() > 0) {
            this.sendMessageInterceptorGroup = new InterceptorGroup();
            for (Interceptor interceptor : sendMessageInterceptors) {
                this.sendMessageInterceptorGroup.registerInterceptor(interceptor);
                log.warn("Send message interceptor: {} registered!", interceptor.interceptorName());
            }
        }

    }

    public void registerProcessor() {
        this.snodeServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor, this.sendMessageExecutor);
        this.snodeServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendMessageProcessor, this.sendMessageExecutor);
        this.snodeServer.registerProcessor(RequestCode.HEART_BEAT, heartbeatProcessor, this.heartbeatExecutor);
        this.snodeServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, heartbeatProcessor, this.heartbeatExecutor);
        this.snodeServer.registerProcessor(RequestCode.SNODE_PULL_MESSAGE, pullMessageProcessor, this.pullMessageExecutor);
        this.snodeServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.snodeServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.snodeServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.snodeServer.registerProcessor(RequestCode.GET_MIN_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.snodeServer.registerProcessor(RequestCode.GET_MAX_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.snodeServer.registerProcessor(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, consumerManageProcessor, this.consumerManageExecutor);
    }

    public void start() {
        initialize();
        this.snodeServer.start();
        this.remotingClient.start();
        this.scheduledService.startScheduleTask();
        this.clientHousekeepingService.start(this.snodeConfig.getHouseKeepingInterval());
    }

    public void shutdown() {
        this.sendMessageExecutor.shutdown();
        this.pullMessageExecutor.shutdown();
        this.heartbeatExecutor.shutdown();
        this.consumerManagerExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.remotingClient.shutdown();
        this.scheduledService.shutdown();
        this.clientHousekeepingService.shutdown();
        this.pushService.shutdown();
    }

    public RemotingServer getSnodeServer() {
        return snodeServer;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public ClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public EnodeService getEnodeService() {
        return enodeService;
    }

    public NnodeService getNnodeService() {
        return nnodeService;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public InterceptorGroup getConsumeMessageInterceptorGroup() {
        return consumeMessageInterceptorGroup;
    }

    public InterceptorGroup getSendMessageInterceptorGroup() {
        return sendMessageInterceptorGroup;
    }

    public PushService getPushService() {
        return pushService;
    }

    public void setNnodeService(NnodeService nnodeService) {
        this.nnodeService = nnodeService;
    }

    public void setRemotingClient(RemotingClient remotingClient) {
        this.remotingClient = remotingClient;
    }

    public void setEnodeService(EnodeService enodeService) {
        this.enodeService = enodeService;
    }

    public InterceptorGroup getRemotingServerInterceptorGroup() {
        return remotingServerInterceptorGroup;
    }

    public void setRemotingServerInterceptorGroup(
        InterceptorGroup remotingServerInterceptorGroup) {
        this.remotingServerInterceptorGroup = remotingServerInterceptorGroup;
    }

    public ClientManager getProducerManager() {
        return producerManager;
    }

    public void setProducerManager(ClientManager producerManager) {
        this.producerManager = producerManager;
    }

    public ClientManager getConsumerManager() {
        return consumerManager;
    }

    public void setConsumerManager(ClientManager consumerManager) {
        this.consumerManager = consumerManager;
    }

    public SubscriptionManager getSubscriptionManager() {
        return subscriptionManager;
    }

    public void setSubscriptionManager(SubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }

    public ClientService getClientService() {
        return clientService;
    }

    public void setClientService(ClientService clientService) {
        this.clientService = clientService;
    }

    public SlowConsumerService getSlowConsumerService() {
        return slowConsumerService;
    }

    public void setSlowConsumerService(SlowConsumerService slowConsumerService) {
        this.slowConsumerService = slowConsumerService;
    }
}
