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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;

public class MessageRequestModeManager extends ConfigManager {

    private transient BrokerController brokerController;

    private ConcurrentHashMap<String/*topic*/, ConcurrentHashMap<String/*consumerGroup*/, SetMessageRequestModeRequestBody>>
        messageRequestModeMap = new ConcurrentHashMap<>();

    public MessageRequestModeManager() {
        // empty construct for decode
    }

    public MessageRequestModeManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void setMessageRequestMode(String topic, String consumerGroup, SetMessageRequestModeRequestBody requestBody) {
        ConcurrentHashMap<String, SetMessageRequestModeRequestBody> consumerGroup2ModeMap = messageRequestModeMap.get(topic);
        if (consumerGroup2ModeMap == null) {
            consumerGroup2ModeMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, SetMessageRequestModeRequestBody>[] pre = new ConcurrentHashMap[1];
            ConcurrentHashMap<String, SetMessageRequestModeRequestBody> finalConsumerGroup2ModeMap = consumerGroup2ModeMap;
            messageRequestModeMap.compute(topic, (key, existingValue) -> {
                if (existingValue == null) {
                    notifyMessageRequestModeCreated(requestBody);
                    pre[0] = null;
                    return finalConsumerGroup2ModeMap;
                } else {
                    notifyMessageRequestModeUpdated(requestBody);
                    pre[0] = existingValue;
                    return existingValue;
                }
            });
            if (pre[0] != null) {
                consumerGroup2ModeMap = pre[0];
            }
        }
        consumerGroup2ModeMap.put(consumerGroup, requestBody);
    }

    private void notifyMessageRequestModeCreated(SetMessageRequestModeRequestBody requestBody) {
        brokerController.getMetadataChangeObserver().onCreated(TopicValidator.RMQ_SYS_MESSAGE_MODE_SYNC, requestBody.getTopic(), requestBody);
    }

    private void notifyMessageRequestModeUpdated(SetMessageRequestModeRequestBody requestBody) {
        brokerController.getMetadataChangeObserver().onUpdated(TopicValidator.RMQ_SYS_MESSAGE_MODE_SYNC, requestBody.getTopic(), requestBody);
    }

    private void notifyMessageRequestModeDeleted(SetMessageRequestModeRequestBody requestBody) {
        brokerController.getMetadataChangeObserver().onDeleted(TopicValidator.RMQ_SYS_MESSAGE_MODE_SYNC, requestBody.getTopic(), requestBody);
    }

    public SetMessageRequestModeRequestBody getMessageRequestMode(String topic, String consumerGroup) {
        ConcurrentHashMap<String, SetMessageRequestModeRequestBody> consumerGroup2ModeMap = messageRequestModeMap.get(topic);
        if (consumerGroup2ModeMap != null) {
            return consumerGroup2ModeMap.get(consumerGroup);
        }

        return null;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> getMessageRequestModeMap() {
        return this.messageRequestModeMap;
    }

    public void setMessageRequestModeMap(ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> messageRequestModeMap) {
        this.messageRequestModeMap = messageRequestModeMap;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getMessageRequestModePath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            MessageRequestModeManager obj = RemotingSerializable.fromJson(jsonString, MessageRequestModeManager.class);
            if (obj != null) {
                this.messageRequestModeMap = obj.messageRequestModeMap;
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> deepCopyMessageRequestModeMapSnapshot() {
        ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> newOuterMap = new ConcurrentHashMap<>(this.messageRequestModeMap.size());
        for (Map.Entry<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> topicEntry : this.messageRequestModeMap.entrySet()) {
            String topic = topicEntry.getKey();
            ConcurrentHashMap<String, SetMessageRequestModeRequestBody> originalInnerMap = topicEntry.getValue();

            if (originalInnerMap != null) {
                ConcurrentHashMap<String, SetMessageRequestModeRequestBody> newInnerMap = new ConcurrentHashMap<>(originalInnerMap.size());
                for (Map.Entry<String, SetMessageRequestModeRequestBody> groupEntry : originalInnerMap.entrySet()) {
                    String consumerGroup = groupEntry.getKey();
                    SetMessageRequestModeRequestBody originalBody = groupEntry.getValue();

                    if (originalBody != null) {
                        try {
                            SetMessageRequestModeRequestBody clonedBody = originalBody.clone();
                            newInnerMap.put(consumerGroup, clonedBody);
                        } catch (CloneNotSupportedException e) {
                            newInnerMap.put(consumerGroup, originalBody);
                        }
                    }
                }
                newOuterMap.put(topic, newInnerMap);
            }
        }
        return newOuterMap;
    }
}
