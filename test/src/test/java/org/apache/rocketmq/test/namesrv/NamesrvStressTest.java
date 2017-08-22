package org.apache.rocketmq.test.namesrv;

import com.google.common.util.concurrent.RateLimiter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.junit.Test;

public class NamesrvStressTest {

    @Test
    public void stressCompress() {
        NamesrvController namesrvController = IntegrationTestBase.createAndStartNamesrv();
        String nsAddr = "127.0.0.1:" + namesrvController.getNettyServerConfig().getListenPort();

        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        BrokerOuterAPI brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        brokerOuterAPI.updateNameServerAddressList(nsAddr);
        brokerOuterAPI.start();
        TopicConfigSerializeWrapper topicConfigWrapper = new TopicConfigSerializeWrapper();
        ConcurrentMap<String, TopicConfig> topicConfigs = topicConfigWrapper.getTopicConfigTable();
        NumberFormat numberFormat = new DecimalFormat("0000000000");
        List<String> topics = new ArrayList<String>();
        for (int i = 0; i < 10240; i++) {
            TopicConfig topicConfig = new TopicConfig("Topic" + numberFormat.format(i));
            topicConfigs.put(topicConfig.getTopicName(), topicConfig);
            topics.add(topicConfig.getTopicName());
        }

        RateLimiter rateLimiter = RateLimiter.create(256);
        for (int i = 0; i < 32; i++) {
            rateLimiter.acquire();
            brokerOuterAPI.registerBrokerAll("TestCluster", "localhost:10911", "broker-a",
                i % 2 , "localhost:10912", topicConfigWrapper, null, false, 3000);
        }
    }
}
