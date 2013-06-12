package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.common.ServiceThread;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class RebalanceService extends ServiceThread {

    @Override
    public void run() {

    }


    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
