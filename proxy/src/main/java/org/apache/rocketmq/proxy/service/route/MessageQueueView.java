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
package org.apache.rocketmq.proxy.service.route;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class MessageQueueView {
    public static final MessageQueueView WRAPPED_EMPTY_QUEUE = new MessageQueueView("", new TopicRouteData(), null);

    private final MessageQueueSelector readSelector;
    private final MessageQueueSelector writeSelector;
    private final TopicRouteWrapper topicRouteWrapper;

    public MessageQueueView(String topic, TopicRouteData topicRouteData, MQFaultStrategy mqFaultStrategy) {
        this.topicRouteWrapper = new TopicRouteWrapper(topicRouteData, topic);

        this.readSelector = new MessageQueueSelector(topicRouteWrapper, mqFaultStrategy, true);
        this.writeSelector = new MessageQueueSelector(topicRouteWrapper, mqFaultStrategy, false);
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteWrapper.getTopicRouteData();
    }

    public TopicRouteWrapper getTopicRouteWrapper() {
        return topicRouteWrapper;
    }

    public String getTopicName() {
        return topicRouteWrapper.getTopicName();
    }

    public boolean isEmptyCachedQueue() {
        return this == WRAPPED_EMPTY_QUEUE;
    }

    public MessageQueueSelector getReadSelector() {
        return readSelector;
    }

    public MessageQueueSelector getWriteSelector() {
        return writeSelector;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("readSelector", readSelector)
            .add("writeSelector", writeSelector)
            .add("topicRouteWrapper", topicRouteWrapper)
            .toString();
    }
}
