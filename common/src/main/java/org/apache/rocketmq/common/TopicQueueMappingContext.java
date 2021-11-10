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
package org.apache.rocketmq.common;

public class TopicQueueMappingContext  {
    private String topic;
    private Integer globalId;
    private Long globalOffset;
    private TopicQueueMappingDetail mappingDetail;
    private LogicQueueMappingItem mappingItem;

    public TopicQueueMappingContext(String topic, Integer globalId, Long globalOffset, TopicQueueMappingDetail mappingDetail, LogicQueueMappingItem mappingItem) {
        this.topic = topic;
        this.globalId = globalId;
        this.globalOffset = globalOffset;
        this.mappingDetail = mappingDetail;
        this.mappingItem = mappingItem;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getGlobalId() {
        return globalId;
    }

    public void setGlobalId(Integer globalId) {
        this.globalId = globalId;
    }

    public Long getGlobalOffset() {
        return globalOffset;
    }

    public void setGlobalOffset(Long globalOffset) {
        this.globalOffset = globalOffset;
    }

    public TopicQueueMappingDetail getMappingDetail() {
        return mappingDetail;
    }

    public void setMappingDetail(TopicQueueMappingDetail mappingDetail) {
        this.mappingDetail = mappingDetail;
    }

    public LogicQueueMappingItem getMappingItem() {
        return mappingItem;
    }

    public void setMappingItem(LogicQueueMappingItem mappingItem) {
        this.mappingItem = mappingItem;
    }
}
