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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.base;

import com.google.common.truth.Truth;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.proxy.ProxyMode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.test.util.MQAdminTestUtils;

import static org.apache.rocketmq.test.base.BaseConf.brokerController1;

public class IntegrationTestBase {
    public static Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    protected static final String SEP = File.separator;
    protected static final String BROKER_NAME_PREFIX = "TestBrokerName_";
    protected static final AtomicInteger BROKER_INDEX = new AtomicInteger(0);
    protected static final List<File> TMPE_FILES = new ArrayList<>();
    protected static final List<BrokerController> BROKER_CONTROLLERS = new ArrayList<>();
    protected static final List<NamesrvController> NAMESRV_CONTROLLERS = new ArrayList<>();
    protected static int topicCreateTime = (int) TimeUnit.SECONDS.toSeconds(30);
    public static volatile int commitLogSize = 1024 * 1024 * 100;
    protected static final int INDEX_NUM = 1000;

    static {

        System.setProperty("rocketmq.client.logRoot", System.getProperty("java.io.tmpdir"));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    for (BrokerController brokerController : BROKER_CONTROLLERS) {
                        if (brokerController != null) {
                            brokerController.shutdown();
                        }
                    }

                    // should destroy message store, otherwise could not delete the temp files.
                    for (BrokerController brokerController : BROKER_CONTROLLERS) {
                        if (brokerController != null) {
                            brokerController.getMessageStore().destroy();
                        }
                    }

                    for (NamesrvController namesrvController : NAMESRV_CONTROLLERS) {
                        if (namesrvController != null) {
                            namesrvController.shutdown();
                        }
                    }
                    for (File file : TMPE_FILES) {
                        UtilAll.deleteFile(file);
                    }
                    MQAdminTestUtils.shutdownAdmin();
                } catch (Exception e) {
                    logger.error("Shutdown error", e);
                }
            }
        });

    }

    public static String createBaseDir() {
        String baseDir = System.getProperty("java.io.tmpdir") + SEP + "unitteststore-" + UUID.randomUUID();
        final File file = new File(baseDir);
        if (file.exists()) {
            logger.info(String.format("[%s] has already existed, please back up and remove it for integration tests", baseDir));
            System.exit(1);
        }
        TMPE_FILES.add(file);
        return baseDir;
    }

    public static NamesrvController createAndStartNamesrv() {
        String baseDir = createBaseDir();
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nameServerNettyServerConfig = new NettyServerConfig();
        namesrvConfig.setKvConfigPath(baseDir + SEP + "namesrv" + SEP + "kvConfig.json");
        namesrvConfig.setConfigStorePath(baseDir + SEP + "namesrv" + SEP + "namesrv.properties");

        nameServerNettyServerConfig.setListenPort(0);
        NamesrvController namesrvController = new NamesrvController(namesrvConfig, nameServerNettyServerConfig);
        try {
            Truth.assertThat(namesrvController.initialize()).isTrue();
            logger.info("Name Server Start:{}", nameServerNettyServerConfig.getListenPort());
            namesrvController.start();
        } catch (Exception e) {
            logger.info("Name Server start failed", e);
            System.exit(1);
        }
        NAMESRV_CONTROLLERS.add(namesrvController);
        return namesrvController;

    }

    public static BrokerController createAndStartBroker(String nsAddr) {
        String baseDir = createBaseDir();
        BrokerConfig brokerConfig = new BrokerConfig();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        brokerConfig.setBrokerName(BROKER_NAME_PREFIX + BROKER_INDEX.incrementAndGet());
        brokerConfig.setBrokerIP1("127.0.0.1");
        brokerConfig.setNamesrvAddr(nsAddr);
        brokerConfig.setEnablePropertyFilter(true);
        brokerConfig.setEnableCalcFilterBitMap(true);
        brokerConfig.setAppendAckAsync(true);
        brokerConfig.setAppendCkAsync(true);
        brokerConfig.setRecallMessageEnable(true);
        storeConfig.setEnableConsumeQueueExt(true);
        brokerConfig.setLoadBalancePollNameServerInterval(500);
        brokerConfig.setPopConsumerKVServiceInit(true);
        brokerConfig.setConfigManagerVersion(System.getProperty("configManagerVersion", "v1"));
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + SEP + "commitlog");
        storeConfig.setMappedFileSizeCommitLog(commitLogSize);
        storeConfig.setMaxIndexNum(INDEX_NUM);
        storeConfig.setMaxHashSlotNum(INDEX_NUM * 4);
        storeConfig.setDeleteWhen("01;02;03;04;05;06;07;08;09;10;11;12;13;14;15;16;17;18;19;20;21;22;23;00");
        storeConfig.setMaxTransferCountOnMessageInMemory(1024);
        storeConfig.setMaxTransferCountOnMessageInDisk(1024);
        storeConfig.setEnableLmq(Boolean.valueOf(System.getProperty("enableLmq", "false")));
        storeConfig.setEnableMultiDispatch(Boolean.valueOf(System.getProperty("enableMultiDispatch", "false")));
        storeConfig.setStoreType(System.getProperty("storeType", "default"));
        return createAndStartBroker(storeConfig, brokerConfig);
    }

    public static void createAndStartProxy(String nsAddr) {
        try {
            ProxyStartAndShutdown startAndShutdown = new ProxyStartAndShutdown();
            ConfigurationManager.initConfig();
            ProxyConfig config = ConfigurationManager.getProxyConfig();
            config.setNamesrvAddr(nsAddr);
            config.setEnableTopicMessageTypeCheck(false);
            ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
                config.getGrpcThreadPoolNums(),
                config.getGrpcThreadPoolNums(),
                1, TimeUnit.MINUTES,
                "GrpcRequestExecutorThread",
                config.getGrpcThreadPoolQueueCapacity()
            );
            startAndShutdown.appendShutdown(executor::shutdown);

            String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
            MessagingProcessor messagingProcessor;
            if (ProxyMode.isClusterMode(proxyModeStr)) {
                messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
            } else {
                messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController1);
            }
            startAndShutdown.appendStartAndShutdown(messagingProcessor);

            TlsCertificateManager tlsCertificateManager = new TlsCertificateManager();
            startAndShutdown.appendStartAndShutdown(tlsCertificateManager);

            GrpcMessagingApplication application = GrpcMessagingApplication.create(messagingProcessor);
            startAndShutdown.appendStartAndShutdown(application);

            GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor,
                    ConfigurationManager.getProxyConfig().getGrpcServerPort(), tlsCertificateManager)
                .addService(application)
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .configInterceptor()
                .shutdownTime(ConfigurationManager.getProxyConfig().getGrpcShutdownTimeSeconds(), TimeUnit.SECONDS)
                .build();
            startAndShutdown.appendStartAndShutdown(grpcServer);

            startAndShutdown.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    startAndShutdown.preShutdown();
                    startAndShutdown.shutdown();
                } catch (Exception e) {
                }
            }));
        } catch (Throwable e) {
            logger.error("proxy start failed, will exit", e);
            System.exit(1);
        }
    }

    public static BrokerController createAndStartBroker(MessageStoreConfig storeConfig, BrokerConfig brokerConfig) {
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(0);
        storeConfig.setHaListenPort(0);
        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, storeConfig);
        try {
            Truth.assertThat(brokerController.initialize()).isTrue();
            logger.info("Broker Start name:{} addr:{}", brokerConfig.getBrokerName(), brokerController.getBrokerAddr());
            brokerController.start();
        } catch (Throwable t) {
            logger.error("Broker start failed, will exit", t);
            System.exit(1);
        }
        BROKER_CONTROLLERS.add(brokerController);
        return brokerController;
    }

    public static boolean initTopic(String topic, String nsAddr, String clusterName, int queueNumbers, CQType cqType) {
        return initTopic(topic, nsAddr, clusterName, queueNumbers, cqType, TopicMessageType.NORMAL, null);
    }

    public static boolean initTopic(String topic, String nsAddr, String clusterName, int queueNumbers,
        CQType cqType, TopicMessageType topicMessageType) {
        return initTopic(topic, nsAddr, clusterName, queueNumbers, cqType, topicMessageType, null);
    }

    public static boolean initTopic(String topic, String nsAddr, String clusterName, int queueNumbers,
        CQType cqType, TopicMessageType topicMessageType, Long liteTtl) {
        boolean createResult;
        Map<String, String> attributes = new HashMap<>();
        if (!Objects.equals(CQType.SimpleCQ, cqType)) {
            attributes.put("+" + TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName(), cqType.toString());
        }
        if (!Objects.equals(TopicMessageType.NORMAL, topicMessageType)) {
            attributes.put("+" + TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), topicMessageType.toString());
        }
        if (Objects.equals(TopicMessageType.LITE, topicMessageType)) {
            attributes.put("+" + TopicAttributes.LITE_EXPIRATION_ATTRIBUTE.getName(), liteTtl.toString());
        }
        createResult = MQAdminTestUtils.createTopic(nsAddr, clusterName, topic, queueNumbers, attributes, topicCreateTime);
        return createResult;
    }

    public static boolean initTopic(String topic, String nsAddr, String clusterName, CQType cqType) {
        return initTopic(topic, nsAddr, clusterName, BaseConf.QUEUE_NUMBERS, cqType, TopicMessageType.NORMAL, null);
    }

    public static boolean initTopic(String topic, String nsAddr, String clusterName,
        TopicMessageType topicMessageType) {
        return initTopic(topic, nsAddr, clusterName, BaseConf.QUEUE_NUMBERS, CQType.SimpleCQ, topicMessageType, null);
    }

    public static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        UtilAll.deleteFile(file);
    }

    public static void initMQAdmin(String nsAddr) {
        try {
            MQAdminTestUtils.startAdmin(nsAddr);
        } catch (MQClientException e) {
            logger.info("MQAdmin start failed");
            System.exit(1);
        }
    }

    private static class ProxyStartAndShutdown extends AbstractStartAndShutdown {
        @Override
        public void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
            super.appendStartAndShutdown(startAndShutdown);
        }
    }
}
