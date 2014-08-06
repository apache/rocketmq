/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.mqtrace;

import java.util.Map;


public class ConsumeMessageContext {
    private String consumerGroup;
    private String topic;
    private Integer queueId;
    private String clientHost;
    private String storeHost;
    private Map<Long, String> messageIds;
    private boolean success;
    private String status;


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public Integer getQueueId() {
        return queueId;
    }


    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }


    public String getClientHost() {
        return clientHost;
    }


    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
    }


    public Map<Long, String> getMessageIds() {
        return messageIds;
    }


    public void setMessageIds(Map<Long, String> messageIds) {
        this.messageIds = messageIds;
    }


    public boolean isSuccess() {
        return success;
    }


    public void setSuccess(boolean success) {
        this.success = success;
    }


    public String getStatus() {
        return status;
    }


    public void setStatus(String status) {
        this.status = status;
    }


    public String getStoreHost() {
        return storeHost;
    }


    public void setStoreHost(String storeHost) {
        this.storeHost = storeHost;
    }
}
