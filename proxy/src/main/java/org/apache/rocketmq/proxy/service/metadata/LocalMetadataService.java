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

package org.apache.rocketmq.proxy.service.metadata;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class LocalMetadataService implements MetadataService {
    private final BrokerController brokerController;

    public LocalMetadataService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public TopicMessageType getTopicMessageType(String topic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig == null) {
            return TopicMessageType.UNSPECIFIED;
        }
        return topicConfig.getTopicMessageType();
    }

    @Override
    public SubscriptionGroupConfig getSubscriptionGroupConfig(String group) {
        return this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().get(group);
    }
}
