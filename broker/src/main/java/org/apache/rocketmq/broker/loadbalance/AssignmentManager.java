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
package org.apache.rocketmq.broker.loadbalance;

import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;

public class AssignmentManager {
    private transient BrokerController brokerController;

    private static final List<String> IGNORE_ROUTE_TOPICS = Lists.newArrayList(
        MixAll.CID_RMQ_SYS_PREFIX,
        MixAll.DEFAULT_CONSUMER_GROUP,
        MixAll.TOOLS_CONSUMER_GROUP,
        MixAll.FILTERSRV_CONSUMER_GROUP,
        MixAll.MONITOR_CONSUMER_GROUP,
        MixAll.ONS_HTTP_PROXY_GROUP,
        MixAll.CID_ONSAPI_PERMISSION_GROUP,
        MixAll.CID_ONSAPI_OWNER_GROUP,
        MixAll.CID_ONSAPI_PULL_GROUP
    );

    private final List<String> ignoreRouteTopics = Lists.newArrayList(IGNORE_ROUTE_TOPICS);

    public AssignmentManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        ignoreRouteTopics.add(brokerController.getBrokerConfig().getBrokerClusterName());
        ignoreRouteTopics.add(brokerController.getBrokerConfig().getBrokerName());
    }

    public void start() {
    }

    public void shutdown() {
    }


    public Set<MessageQueue> getTopicSubscribeInfo(String topic) {
        return this.brokerController.getTopicRouteInfoManager().getTopicSubscribeInfo(topic);
    }

    public Set<String> getTopicSetForAssignment() {
        final Set<String> topicSet = new HashSet<>(brokerController.getTopicConfigManager().getTopicConfigTable().keySet());

        for (Iterator<String> it = topicSet.iterator(); it.hasNext(); ) {
            String topic = it.next();
            if (TopicValidator.isSystemTopic(topic)) {
                it.remove();
            } else {
                for (String keyword : ignoreRouteTopics) {
                    if (topic.contains(keyword)) {
                        it.remove();
                        break;
                    }
                }
            }
        }

        return topicSet;
    }
}
