package com.alibaba.rocketmq.test.integration;

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class BaseTest {
    protected static final String NAME_SERVER_LIST = "10.232.26.122:9876";
    protected static BrokerController brokerController;
    protected String instanceName = "qatest-" + new Exception().getStackTrace()[1].getClassName();
    protected String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if (StringUtils.isNotBlank(NAME_SERVER_LIST)) {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, NAME_SERVER_LIST);
        }
        else {
            initNameServerAndBroker();
        }
    }


    // 初始化nameserver & broker
    public static void initNameServerAndBroker() throws Exception {
        // 设置当前程序版本号，每次发布版本时，都要修改CurrentVersion
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));
        try {
            // 初始化配置文件
            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            nettyServerConfig.setListenPort(10911);
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            // 清理环境
            // deleteDir(System.getProperty("user.home") + File.separator +
            // "store");

            if (null == brokerConfig.getRocketmqHome()) {
                System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            // BrokerId的处理
            switch (messageStoreConfig.getBrokerRole()) {
            case ASYNC_MASTER:
            case SYNC_MASTER:
                // Master Id必须是0
                brokerConfig.setBrokerId(MixAll.MASTER_ID);
                break;
            case SLAVE:
                // Slave Id由Slave监听IP、端口决定
                long id =
                        MixAll.createBrokerId(brokerConfig.getBrokerIP1(), nettyServerConfig.getListenPort());
                brokerConfig.setBrokerId(id);
                break;
            default:
                break;
            }

            // Master监听Slave请求的端口，默认为服务端口+1
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);

            // 初始化Logback
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/log4j_broker.xml");
            final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

            // 打印启动参数
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            // 初始化服务控制对象
            brokerController =
                    new BrokerController(brokerConfig, nettyServerConfig, new NettyClientConfig(),
                        messageStoreConfig);
            boolean initResult = brokerController.initialize();
            if (!initResult) {
                brokerController.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);


                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            brokerController.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            // 启动服务控制对象
            brokerController.start();
            String tip =
                    "The broker[" + brokerController.getBrokerConfig().getBrokerName() + ", "
                            + brokerController.getBrokerAddr() + "] boot success.";
            log.info(tip);
            System.out.println(tip);
        }
        catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    @AfterClass
    public static void tearDown() throws Exception {
        if (brokerController != null) {
            brokerController.shutdown();
        }
    }


    public String getTagsExpression(int count) {
        if (count == 0) {
            return "*";
        }
        else {
            StringBuilder sb = new StringBuilder();
            String splitor = "";
            Random random = new Random();
            for (int i = 0; i < count; i++) {
                sb.append(tags[random.nextInt(tags.length)]);
                sb.append(splitor);
                splitor = " || ";
            }
            return sb.toString();
        }

    }


    public static void deleteDir(String path) {
        File file = new File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                for (File subFile : files) {
                    if (subFile.isDirectory())
                        deleteDir(subFile.getPath());
                    else
                        subFile.delete();
                }
            }
            file.delete();
        }
    }
}
