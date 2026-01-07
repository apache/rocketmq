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

package org.apache.rocketmq.broker.lite;

import io.netty.channel.Channel;

import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.OffsetOption;

public interface LiteSubscriptionRegistry {

    void updateClientChannel(String clientId, Channel channel);

    LiteSubscription getLiteSubscription(String clientId);

    int getActiveSubscriptionNum();

    void addPartialSubscription(String clientId, String group, String topic, Set<String> lmqNameSet, OffsetOption offsetOption);

    void removePartialSubscription(String clientId, String group, String topic, Set<String> lmqNameSet);

    void addCompleteSubscription(String clientId, String group, String topic, Set<String> newLmqNameSet, long version);

    void removeCompleteSubscription(String clientId);

    void addListener(LiteCtlListener listener);

    Set<ClientGroup> getSubscriber(String lmqName);

    List<String> getAllClientIdByGroup(String group);

    void cleanSubscription(String lmqName, boolean notifyClient);

    void start();

    void shutdown();
}
