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

import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

public interface AdminService {

    boolean topicExist(String topic);

    boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic, int wQueueNum,
        int rQueueNum, boolean examineTopic, int retryCheckCount);

    boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum, List<BrokerData> curBrokerDataList,
        List<BrokerData> sampleBrokerDataList, boolean examineTopic, int retryCheckCount) throws Exception;

    // =========================================================================
    // RIP-2 Admin: broker-facing gateway methods.
    //
    // IMPORTANT: every implementation delegates to the proxy's OWN managed
    // broker client (rocketmq-proxy's MQClientAPIFactory). The proxy is the
    // single entry point ("走 grpc 的 proxy"); these calls never open a direct
    // link to the broker from the admin code itself.
    // =========================================================================

    long getMaxOffset(String brokerAddr, MessageQueue messageQueue, long timeoutMillis) throws Exception;

    long getMinOffset(String brokerAddr, MessageQueue messageQueue, long timeoutMillis) throws Exception;

    long getEarliestMsgStoretime(String brokerAddr, MessageQueue messageQueue, long timeoutMillis) throws Exception;

    ConsumeStats fetchConsumeStats(String brokerAddr, String consumerGroup, String topic, long timeoutMillis) throws Exception;

    Map<MessageQueue, Long> resetOffset(String brokerAddr, String topic, String group, long timestamp,
        boolean isForce, long timeoutMillis) throws Exception;

    List<MessageExt> queryMessage(String brokerAddr, String topic, String key, int maxNum,
        long beginTimestamp, long endTimestamp, long timeoutMillis) throws Exception;

    MessageExt viewMessage(String brokerAddr, String topic, long phyoffset, long timeoutMillis) throws Exception;

    org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping getTopicConfig(String brokerAddr, String topic, long timeoutMillis) throws Exception;

    org.apache.rocketmq.remoting.protocol.route.TopicRouteData getTopicRouteData(String topic) throws Exception;
}
