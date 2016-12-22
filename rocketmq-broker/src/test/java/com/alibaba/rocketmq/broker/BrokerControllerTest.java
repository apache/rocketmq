package com.alibaba.rocketmq.broker;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author shtykh_roman
 */
public class BrokerControllerTest {
    private static final int RESTART_NUM = 3;

    /**
     * Tests if the controller can be properly stopped and started.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testRestart() throws Exception {

        for (int i=0; i<RESTART_NUM; i++) {
            BrokerController brokerController = new BrokerController(//
                new BrokerConfig(), //
                new NettyServerConfig(), //
                new NettyClientConfig(), //
                new MessageStoreConfig());
            boolean initResult = brokerController.initialize();
            System.out.println("initialize " + initResult);
            brokerController.start();

            brokerController.shutdown();
        }
    }
}
