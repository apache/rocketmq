/**
 * $Id: NamesrvController.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.namesrv.daemon.NamesrvClient;
import com.alibaba.rocketmq.namesrv.daemon.NamesrvSync;
import com.alibaba.rocketmq.namesrv.daemon.PollingAddress;
import com.alibaba.rocketmq.namesrv.processor.AllRequestProcessor;
import com.alibaba.rocketmq.namesrv.topic.DefaultNamesrvConfigManager;
import com.alibaba.rocketmq.namesrv.topic.DefaultTopicRuntimeDataManager;
import com.alibaba.rocketmq.namesrv.topic.NamesrvConfigManager;
import com.alibaba.rocketmq.namesrv.topic.TopicRuntimeDataManager;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;


/**
 * Name server 各个服务控制器
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * @author lansheng.zj@taobao.com
 */
public class NamesrvController {
    private static final Logger log = LoggerFactory.getLogger(MixAll.NamesrvLoggerName);
    // Name Server配置
    private final NamesrvConfig namesrvConfig;
    // 通信层配置
    private final NettyServerConfig nettyServerConfig;

    // nameserver服务端通信层对象
    private RemotingServer remotingServer;

    // nameserver服务端网络请求处理线程池
    private ExecutorService requestsServerExecutor;

    private TopicRuntimeDataManager topicInfoManager;
    private NamesrvConfigManager namesrvConfigManager;

    // namesrv结点间数据同步
    private NamesrvSync namesrvSync;
    // namesrv拉取broker数据
    private NamesrvClient namesrvClient;
    // 轮询获取namesrv地址
    private PollingAddress pollingAddress;


    public NamesrvController(final NamesrvConfig namesrvConfig, final NettyServerConfig nettyServerConfig,
            final NettyClientConfig nettyClientConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        topicInfoManager = new DefaultTopicRuntimeDataManager(namesrvConfig);
        namesrvConfigManager = new DefaultNamesrvConfigManager(namesrvConfig);
        namesrvSync = new NamesrvSync(namesrvConfig, topicInfoManager);
        namesrvClient = new NamesrvClient(namesrvConfig, nettyClientConfig, topicInfoManager);
        pollingAddress = new PollingAddress(namesrvConfig);
    }


    public boolean initialize() {
        // 打印服务器配置参数
        MixAll.printObjectProperties(log, namesrvConfig);

        // 初始化通信层
        remotingServer = new NettyRemotingServer(nettyServerConfig);

        // 初始化线程池
        requestsServerExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);


                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "RequestsExecutorThread_" + threadIndex.incrementAndGet());
                    }
                });

        registerProcessor();

        return topicInfoManager.init();
    }


    public void registerProcessor() {
        remotingServer.registerDefaultProcessor(new AllRequestProcessor(this, topicInfoManager),
            this.requestsServerExecutor);
    }


    public void start() throws Exception {
        remotingServer.start();
        namesrvSync.start();
        namesrvClient.start();
        pollingAddress.start();
    }


    public void shutdown() {
        requestsServerExecutor.shutdown();
        remotingServer.shutdown();
        namesrvSync.shutdown();
        topicInfoManager.shutdown();
        namesrvClient.shutdown();
        pollingAddress.shutdown();
    }
}
