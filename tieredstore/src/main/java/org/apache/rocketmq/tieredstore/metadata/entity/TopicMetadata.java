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
package org.apache.rocketmq.tieredstore.metadata.entity;

import com.alibaba.fastjson.annotation.JSONField;

public class TopicMetadata {

    @JSONField(ordinal = 1)
    private long topicId;

    @JSONField(ordinal = 2)
    private String topic;

    @JSONField(ordinal = 3)
    private int status;

    @JSONField(ordinal = 4)
    private long reserveTime;

    @JSONField(ordinal = 5)
    private long updateTimestamp;

    // default constructor is used by fastjson
    @SuppressWarnings("unused")
    public TopicMetadata() {
    }

    public TopicMetadata(long topicId, String topic, long reserveTime) {
        this.topicId = topicId;
        this.topic = topic;
        this.reserveTime = reserveTime;
        this.updateTimestamp = System.currentTimeMillis();
    }

    public long getTopicId() {
        return topicId;
    }

    public void setTopicId(long topicId) {
        this.topicId = topicId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getReserveTime() {
        return reserveTime;
    }

    public void setReserveTime(long reserveTime) {
        this.reserveTime = reserveTime;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }
}
