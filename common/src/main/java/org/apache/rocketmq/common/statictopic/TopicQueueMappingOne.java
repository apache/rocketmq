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
package org.apache.rocketmq.common.statictopic;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;

public class TopicQueueMappingOne extends RemotingSerializable {

    String topic; // redundant field
    String bname;  //identify the hosted broker name
    Integer globalId;
    List<LogicQueueMappingItem> items;

    public TopicQueueMappingOne(String topic, String bname, Integer globalId, List<LogicQueueMappingItem> items) {
        this.topic = topic;
        this.bname = bname;
        this.globalId = globalId;
        this.items = items;
    }

    public String getTopic() {
        return topic;
    }

    public String getBname() {
        return bname;
    }

    public Integer getGlobalId() {
        return globalId;
    }

    public List<LogicQueueMappingItem> getItems() {
        return items;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof TopicQueueMappingOne)) return false;

        TopicQueueMappingOne that = (TopicQueueMappingOne) o;

        return new EqualsBuilder()
                .append(topic, that.topic)
                .append(bname, that.bname)
                .append(globalId, that.globalId)
                .append(items, that.items)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(topic)
                .append(bname)
                .append(globalId)
                .append(items)
                .toHashCode();
    }
}
