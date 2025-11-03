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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class LiteMetadataUtil {

    public static boolean isConsumeEnable(String group, BrokerController brokerController) {
        if (null == group || null == brokerController) {
            return false;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        return null != groupConfig && groupConfig.isConsumeEnable();
    }

    public static boolean isLiteMessageType(String parentTopic, BrokerController brokerController) {
        if (null == parentTopic || null == brokerController) {
            return false;
        }
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(parentTopic);
        return topicConfig != null && TopicMessageType.LITE.equals(topicConfig.getTopicMessageType());
    }

    public static boolean isLiteGroupType(String group, BrokerController brokerController) {
        if (null == group || null == brokerController) {
            return false;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        return null != groupConfig && groupConfig.getLiteBindTopic() != null;
    }

    public static String getLiteBindTopic(String group, BrokerController brokerController) {
        if (null == group || null == brokerController) {
            return null;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        return null != groupConfig ? groupConfig.getLiteBindTopic() : null;
    }

    public static boolean isSubLiteExclusive(String group, BrokerController brokerController) {
        if (null == group || null == brokerController) {
            return false;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        return null != groupConfig && groupConfig.isLiteSubExclusive();
    }

    public static boolean isResetOffsetInExclusiveMode(String group, BrokerController brokerController) {
        if (null == group || null == brokerController) {
            return false;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        return null != groupConfig && groupConfig.isResetOffsetInExclusiveMode();
    }

    public static boolean isResetOffsetOnUnsubscribe(String group, BrokerController brokerController) {
        if (null == group || null == brokerController) {
            return false;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        return null != groupConfig && groupConfig.isResetOffsetOnUnsubscribe();
    }

    public static int getMaxClientEventCount(String group, BrokerController brokerController) {
        if (null == group || null == brokerController) {
            return -1;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        if (null == groupConfig || groupConfig.getMaxClientEventCount() <= 0) {
            return brokerController.getBrokerConfig().getMaxClientEventCount();
        }
        return groupConfig.getMaxClientEventCount();
    }

    public static Map<String, Integer> getTopicTtlMap(BrokerController brokerController) {
        if (null == brokerController) {
            return Collections.emptyMap();
        }
        ConcurrentMap<String, TopicConfig> topicConfigTable =
            brokerController.getTopicConfigManager().getTopicConfigTable();

        return topicConfigTable.entrySet().stream()
            .filter(entry -> entry.getValue().getTopicMessageType().equals(TopicMessageType.LITE))
            .collect(Collectors.toMap(
                entry -> entry.getKey(),
                entry -> entry.getValue().getLiteTopicExpiration()
            ));
    }

    public static Map<String, Set<String>> getSubscriberGroupMap(BrokerController brokerController) {
        if (null == brokerController) {
            return Collections.emptyMap();
        }
        ConcurrentMap<String, SubscriptionGroupConfig> groupTable =
            brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable();

        return groupTable.entrySet().stream()
            .filter(entry -> entry.getValue().getLiteBindTopic() != null)
            .collect(Collectors.groupingBy(
                entry -> entry.getValue().getLiteBindTopic(),
                Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
            ));
    }
}
