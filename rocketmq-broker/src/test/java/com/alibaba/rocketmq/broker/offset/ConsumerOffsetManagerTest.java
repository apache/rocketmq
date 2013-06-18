/**
 * $Id: ConsumerOffsetManagerTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.offset;

import java.util.Random;

import org.junit.Test;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ConsumerOffsetManagerTest {
    @Test
    public void test_flushConsumerOffset() throws Exception {
        BrokerController brokerController =
                new BrokerController(new BrokerConfig(), new NettyServerConfig(), new MessageStoreConfig());
        boolean initResult = brokerController.initialize();
        System.out.println("initialize " + initResult);
        brokerController.start();

        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);

        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            String group = "DIANPU_GROUP_" + i;
            for (int id = 0; id < 16; id++) {
                consumerOffsetManager.commitOffset(group, "TOPIC_A", id,
                    random.nextLong() % 1024 * 1024 * 1024);
                consumerOffsetManager.commitOffset(group, "TOPIC_B", id,
                    random.nextLong() % 1024 * 1024 * 1024);
                consumerOffsetManager.commitOffset(group, "TOPIC_C", id,
                    random.nextLong() % 1024 * 1024 * 1024);
            }
        }

        consumerOffsetManager.persist();

        brokerController.shutdown();
    }


    @Test
    public void test_flushConsumerOffsetHistory() throws Exception {
        BrokerController brokerController =
                new BrokerController(new BrokerConfig(), new NettyServerConfig(), new MessageStoreConfig());
        boolean initResult = brokerController.initialize();
        System.out.println("initialize " + initResult);
        brokerController.start();

        ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(brokerController);

        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            String group = "DIANPU_GROUP_" + i;
            for (int id = 0; id < 16; id++) {
                consumerOffsetManager.commitOffset(group, "TOPIC_A", id,
                    random.nextLong() % 1024 * 1024 * 1024);
                consumerOffsetManager.commitOffset(group, "TOPIC_B", id,
                    random.nextLong() % 1024 * 1024 * 1024);
                consumerOffsetManager.commitOffset(group, "TOPIC_C", id,
                    random.nextLong() % 1024 * 1024 * 1024);
            }
        }

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000 * 3);
        }

        brokerController.shutdown();
    }
}
