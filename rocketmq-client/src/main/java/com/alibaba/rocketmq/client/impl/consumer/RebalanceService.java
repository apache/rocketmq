package com.alibaba.rocketmq.client.impl.consumer;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.ServiceThread;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class RebalanceService extends ServiceThread {
    private final Logger log = ClientLogger.getLog();
    private final MQClientFactory mqClientFactory;


    public RebalanceService(MQClientFactory mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    private static long WaitInterval = 1000 * 10;


    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStoped()) {
            this.waitForRunning(WaitInterval);
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
