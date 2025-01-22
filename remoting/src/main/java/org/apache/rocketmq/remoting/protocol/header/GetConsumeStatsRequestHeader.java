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
package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicRequestHeader;

@RocketMQAction(value = RequestCode.GET_CONSUME_STATS, action = Action.GET)
public class GetConsumeStatsRequestHeader extends TopicRequestHeader {
    private static final String TOPIC_NAME_SEPARATOR = ";";

    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;

    @RocketMQResource(ResourceType.TOPIC)
    private String topic;

    // if topicList is provided, topic will be ignored
    @RocketMQResource(value = ResourceType.TOPIC, splitter = TOPIC_NAME_SEPARATOR)
    private String topicList;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public List<String> fetchTopicList() {
        if (StringUtils.isBlank(topicList)) {
            return Collections.emptyList();
        }
        return Arrays.asList(StringUtils.split(topicList, TOPIC_NAME_SEPARATOR));
    }

    public void updateTopicList(List<String> topicList) {
        if (topicList == null) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        topicList.forEach(topic -> sb.append(topic).append(TOPIC_NAME_SEPARATOR));
        this.setTopicList(sb.toString());
    }

    public String getTopicList() {
        return topicList;
    }

    public void setTopicList(String topicList) {
        this.topicList = topicList;
    }

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("consumerGroup", consumerGroup)
            .add("topic", topic)
            .toString();
    }
}
