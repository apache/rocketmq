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
package org.apache.rocketmq.broker.subscription;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class LmqSubscriptionGroupManager extends SubscriptionGroupManager {

    public LmqSubscriptionGroupManager(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        if (MixAll.isLmq(group)) {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(group);
            return subscriptionGroupConfig;
        }
        return super.findSubscriptionGroupConfig(group);
    }

    @Override
    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        if (config == null || MixAll.isLmq(config.getGroupName())) {
            return;
        }
        super.updateSubscriptionGroupConfig(config);
    }
}
