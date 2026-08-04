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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;

public class DefaultAdminService implements AdminService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
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

            try {
                this.getClient().createTopic(addr, TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, topicConfig, Duration.ofSeconds(3).toMillis());
            } catch (Exception e) {
                log.error("create topic on broker failed. topic:{}, broker:{}", topicConfig, addr, e);
            }
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

    // =========================================================================
    // RIP-2 Admin: broker-facing gateway methods.
    // Every call goes through the proxy's OWN managed broker client.
    // =========================================================================

    @Override
    public long getMaxOffset(String brokerAddr, MessageQueue messageQueue, long timeoutMillis) throws Exception {
        return this.getClient().getMaxOffset(brokerAddr, messageQueue, timeoutMillis);
    }

    @Override
    public long getMinOffset(String brokerAddr, MessageQueue messageQueue, long timeoutMillis) throws Exception {
        return this.getClient().getMinOffset(brokerAddr, messageQueue, timeoutMillis);
    }

    @Override
    public long getEarliestMsgStoretime(String brokerAddr, MessageQueue messageQueue, long timeoutMillis) throws Exception {
        return this.getClient().getEarliestMsgStoretime(brokerAddr, messageQueue, timeoutMillis);
    }

    @Override
    public ConsumeStats fetchConsumeStats(String brokerAddr, String consumerGroup, String topic, long timeoutMillis) throws Exception {
        return this.getClient().getConsumeStats(brokerAddr, consumerGroup, topic, timeoutMillis);
    }

    @Override
    public Map<MessageQueue, Long> resetOffset(String brokerAddr, String topic, String group, long timestamp,
        boolean isForce, long timeoutMillis) throws Exception {
        return this.getClient().invokeBrokerToResetOffset(brokerAddr, topic, group, timestamp, isForce, timeoutMillis);
    }

    @Override
    public MessageExt viewMessage(String brokerAddr, String topic, long phyoffset, long timeoutMillis) throws Exception {
        return this.getClient().viewMessage(brokerAddr, topic, phyoffset, timeoutMillis);
    }

    @Override
    public org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping getTopicConfig(String brokerAddr, String topic, long timeoutMillis) throws Exception {
        return this.getClient().getTopicConfig(brokerAddr, topic, timeoutMillis);
    }

    @Override
    public org.apache.rocketmq.remoting.protocol.route.TopicRouteData getTopicRouteData(String topic) throws Exception {
        return this.getTopicRouteDataDirectlyFromNameServer(topic);
    }

    @Override
    public List<MessageExt> queryMessage(String brokerAddr, String topic, String key, int maxNum,
        long beginTimestamp, long endTimestamp, long timeoutMillis) throws Exception {
        QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setKey(key);
        requestHeader.setMaxNum(maxNum);
        requestHeader.setBeginTimestamp(beginTimestamp);
        requestHeader.setEndTimestamp(endTimestamp);

        CompletableFuture<List<MessageExt>> future = new CompletableFuture<>();
        this.getClient().queryMessage(brokerAddr, requestHeader, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {
                try {
                    RemotingCommand response = responseFuture.getResponseCommand();
                    if (response != null && response.getCode() == ResponseCode.SUCCESS && response.getBody() != null) {
                        List<MessageExt> messageList = MessageDecoder.decodes(
                            java.nio.ByteBuffer.wrap(response.getBody()), true);
                        future.complete(messageList);
                    } else {
                        future.complete(new ArrayList<>());
                    }
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            }

            @Override
            public void operationFail(Throwable e) {
                future.completeExceptionally(e);
            }
        }, false);
        return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
    }
}
