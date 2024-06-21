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
package org.apache.rocketmq.remoting.protocol.statictopic;

import com.google.common.collect.ImmutableList;
import java.util.List;

public class TopicQueueMappingContext  {
    private String topic;
    private Integer globalId;
    private TopicQueueMappingDetail mappingDetail;
    private List<LogicQueueMappingItem> mappingItemList;
    private LogicQueueMappingItem leaderItem;

    private LogicQueueMappingItem currentItem;

    public TopicQueueMappingContext(String topic, Integer globalId, TopicQueueMappingDetail mappingDetail, List<LogicQueueMappingItem> mappingItemList, LogicQueueMappingItem leaderItem) {
        this.topic = topic;
        this.globalId = globalId;
        this.mappingDetail = mappingDetail;
        this.mappingItemList = mappingItemList;
        this.leaderItem = leaderItem;

    }


    public boolean isLeader() {
        return leaderItem != null && leaderItem.getBname().equals(mappingDetail.getBname());
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


    public TopicQueueMappingDetail getMappingDetail() {
        return mappingDetail;
    }

    public void setMappingDetail(TopicQueueMappingDetail mappingDetail) {
        this.mappingDetail = mappingDetail;
    }

    public List<LogicQueueMappingItem> getMappingItemList() {
        return mappingItemList;
    }

    public void setMappingItemList(ImmutableList<LogicQueueMappingItem> mappingItemList) {
        this.mappingItemList = mappingItemList;
    }

    public LogicQueueMappingItem getLeaderItem() {
        return leaderItem;
    }

    public void setLeaderItem(LogicQueueMappingItem leaderItem) {
        this.leaderItem = leaderItem;
    }

    public LogicQueueMappingItem getCurrentItem() {
        return currentItem;
    }

    public void setCurrentItem(LogicQueueMappingItem currentItem) {
        this.currentItem = currentItem;
    }

    public void setMappingItemList(List<LogicQueueMappingItem> mappingItemList) {
        this.mappingItemList = mappingItemList;
    }
}
