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
import java.util.Set;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public interface ConsumerManagerInterface {

    ClientChannelInfo findChannel(String group, String clientId);

    ClientChannelInfo findChannel(String group, Channel channel);

    SubscriptionData findSubscriptionData(String group, String topic);

    ConsumerGroupInfo getConsumerGroupInfo(String group);

    int findSubscriptionDataCount(String group);

    boolean doChannelCloseEvent(String remoteAddr, Channel channel);

    default boolean registerConsumer(String group, ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {
        return registerConsumer(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList,
            isNotifyConsumerIdsChangedEnable, true);
    }

    boolean registerConsumer(String group, ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable, boolean updateSubscription);

    void unregisterConsumer(String group, ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable);

    void scanNotActiveChannel();

    Set<String> queryTopicConsumeByWho(String topic);

    void appendConsumerIdsChangeListener(ConsumerIdsChangeListener listener);
}
