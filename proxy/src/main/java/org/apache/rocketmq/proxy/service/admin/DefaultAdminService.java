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

package org.apache.rocketmq.proxy.service.admin;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIExt;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;

public class DefaultAdminService implements AdminService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final MQClientAPIFactory mqClientAPIFactory;

    public DefaultAdminService(MQClientAPIFactory mqClientAPIFactory) {
        this.mqClientAPIFactory = mqClientAPIFactory;
    }

    @Override
    public boolean topicExist(String topic) {
        boolean topicExist;
        TopicRouteData topicRouteData;
        try {
            topicRouteData = this.getTopicRouteDataDirectlyFromNameServer(topic);
            topicExist = topicRouteData != null;
        } catch (Throwable e) {
            topicExist = false;
        }

        return topicExist;
    }

    @Override
    public boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic, int wQueueNum,
        int rQueueNum, boolean examineTopic, int retryCheckCount) {
        TopicRouteData curTopicRouteData = new TopicRouteData();
        try {
            curTopicRouteData = this.getTopicRouteDataDirectlyFromNameServer(createTopic);
        } catch (Exception e) {
            if (!TopicRouteHelper.isTopicNotExistError(e)) {
                log.error("get cur topic route {} failed.", createTopic, e);
                return false;
            }
        }

        TopicRouteData sampleTopicRouteData = null;
        try {
            sampleTopicRouteData = this.getTopicRouteDataDirectlyFromNameServer(sampleTopic);
        } catch (Exception e) {
            log.error("create topic {} failed.", createTopic, e);
            return false;
        }

        if (sampleTopicRouteData == null || sampleTopicRouteData.getBrokerDatas().isEmpty()) {
            return false;
        }

        try {
            return this.createTopicOnBroker(createTopic, wQueueNum, rQueueNum, curTopicRouteData.getBrokerDatas(),
                sampleTopicRouteData.getBrokerDatas(), examineTopic, retryCheckCount);
        } catch (Exception e) {
            log.error("create topic {} failed.", createTopic, e);
        }
        return false;
    }

    @Override
    public boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum, List<BrokerData> curBrokerDataList,
        List<BrokerData> sampleBrokerDataList, boolean examineTopic, int retryCheckCount) throws Exception {
        Set<String> curBrokerAddr = new HashSet<>();
        if (curBrokerDataList != null) {
            for (BrokerData brokerData : curBrokerDataList) {
                curBrokerAddr.add(brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
            }
        }

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setWriteQueueNums(wQueueNum);
        topicConfig.setReadQueueNums(rQueueNum);
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);

        for (BrokerData brokerData : sampleBrokerDataList) {
            String addr = brokerData.getBrokerAddrs() == null ? null : brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (addr == null) {
                continue;
            }
            if (curBrokerAddr.contains(addr)) {
                continue;
            }

            this.getClient().createTopic(addr, TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, topicConfig, Duration.ofSeconds(3).toMillis());
        }

        if (examineTopic) {
            // examine topic exist.
            int count = retryCheckCount;
            while (count-- > 0) {
                if (this.topicExist(topic)) {
                    return true;
                }
            }
        } else {
            return true;
        }
        return false;
    }

    protected TopicRouteData getTopicRouteDataDirectlyFromNameServer(String topic) throws Exception {
        return this.getClient().getTopicRouteInfoFromNameServer(topic, Duration.ofSeconds(3).toMillis());
    }

    protected MQClientAPIExt getClient() {
        return this.mqClientAPIFactory.getClient();
    }
}
