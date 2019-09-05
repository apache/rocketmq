package org.apache.rocketmq.ekfet;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerControllerTest {
    private static final BrokerConfig brokerConfig;
    private static final NettyServerConfig nettyServerConfig;
    private static final MessageStoreConfig messageStoreConfig;

    static {
        brokerConfig = new BrokerConfig();
        brokerConfig.setNamesrvAddr("127.0.0.1:7986");
        brokerConfig.setBrokerName("broker-a");

        nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);

        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setDeleteWhen("04");
        messageStoreConfig.setFileReservedTime(48);
        messageStoreConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        messageStoreConfig.setDuplicationEnable(false);
    }

    public static void main(String[] args) {
        BrokerControllerTest brokerControllerTest = new BrokerControllerTest();
        brokerControllerTest.start();
    }

    private synchronized void start() {
        try {
            BrokerController brokerController = new BrokerController(
                    brokerConfig,
                    nettyServerConfig,
                    new NettyClientConfig(),
                    messageStoreConfig);
            brokerController.initialize();
            brokerController.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
