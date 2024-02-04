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

import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
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
            ConcurrentHashMap<String, SetMessageRequestModeRequestBody> pre =
                messageRequestModeMap.putIfAbsent(topic, consumerGroup2ModeMap);
            if (pre != null) {
                consumerGroup2ModeMap = pre;
            }
        }
        consumerGroup2ModeMap.put(consumerGroup, requestBody);
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
}
