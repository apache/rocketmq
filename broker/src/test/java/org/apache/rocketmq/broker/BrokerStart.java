package org.apache.rocketmq.broker;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerStart {
    public static void main(String[] args) throws Exception {

        // 设置版本号
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        // NettyServerConfig 配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);
        // BrokerConfig 配置
        final BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("broker-a");
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");
        // MessageStoreConfig 配置
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setDeleteWhen("04");
        messageStoreConfig.setFileReservedTime(48);
        messageStoreConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        messageStoreConfig.setDuplicationEnable(false);

        BrokerController brokerController = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                new NettyClientConfig(),
                messageStoreConfig
        );
        assertThat(brokerController.initialize());
        brokerController.start();
        // 睡觉，就不起来
        System.out.println("你猜");
        Thread.sleep(DateUtils.MILLIS_PER_DAY);
    }
}
