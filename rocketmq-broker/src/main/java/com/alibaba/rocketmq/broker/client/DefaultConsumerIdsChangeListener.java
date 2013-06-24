package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.List;

import com.alibaba.rocketmq.broker.BrokerController;


public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    private final BrokerController brokerController;


    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public void consumerIdsChanged(String group, List<Channel> channels) {
        if (channels != null) {
            for (Channel chl : channels) {
                this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
            }
        }
    }
}
