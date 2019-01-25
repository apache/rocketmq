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
package org.apache.rocketmq.snode.client;

import java.util.Set;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.snode.client.impl.Subscription;

public interface SubscriptionManager {
    boolean subscribe(String groupId, Set<SubscriptionData> subscriptionDataSet, ConsumeType consumeType,
        MessageModel messageModel, ConsumeFromWhere consumeFromWhere);

    void unSubscribe(String groupId, RemotingChannel remotingChannel,
        Set<SubscriptionData> subscriptionDataSet);

    void cleanSubscription(String groupId, String topic);

    Subscription getSubscription(String groupId);

    void registerPushSession(Set<SubscriptionData> subscriptionDataSet, RemotingChannel remotingChannel,
        String groupId);

    void removePushSession(RemotingChannel remotingChannel);

    Set<RemotingChannel> getPushableChannel(MessageQueue messageQueue);
}
