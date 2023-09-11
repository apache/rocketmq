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
package org.apache.rocketmq.store.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class MultiDispatch {

    public static String lmqQueueKey(String queueName) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = 0;
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    public static boolean isNeedHandleMultiDispatch(MessageStoreConfig messageStoreConfig, String topic) {
        return messageStoreConfig.isEnableMultiDispatch()
            && !topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            && !topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)
            && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }

    public static boolean checkMultiDispatchQueue(MessageStoreConfig messageStoreConfig, DispatchRequest dispatchRequest) {
        if (!isNeedHandleMultiDispatch(messageStoreConfig, dispatchRequest.getTopic())) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    public static List<DispatchRequest> checkMultiDispatchQueue(MessageStoreConfig messageStoreConfig, List<DispatchRequest> dispatchRequests) {
        if (!messageStoreConfig.isEnableMultiDispatch() || dispatchRequests == null || dispatchRequests.size() == 0) {
            return null;
        }
        List<DispatchRequest> result = new ArrayList<>();
        for (DispatchRequest dispatchRequest : dispatchRequests) {
            if (checkMultiDispatchQueue(messageStoreConfig, dispatchRequest)) {
                result.add(dispatchRequest);
            }
        }
        return dispatchRequests;
    }
}
