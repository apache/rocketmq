/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    private final BrokerController brokerController;

    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                if (args == null || args.length < 1) {
                    return;
                }
                List<Channel> channels = (List<Channel>) args[0];
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channels) {
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                    }
                }
                break;
            case UNREGISTER:
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }
}
