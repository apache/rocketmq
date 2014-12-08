/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.List;

import com.alibaba.rocketmq.broker.BrokerController;


/**
 * ConsumerId列表变化，通知所有Consumer
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    private final BrokerController brokerController;


    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public void consumerIdsChanged(String group, List<Channel> channels) {
        if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
            for (Channel chl : channels) {
                this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
            }
        }
    }
}
