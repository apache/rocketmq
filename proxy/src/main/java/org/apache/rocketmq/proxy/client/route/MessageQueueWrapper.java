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
package org.apache.rocketmq.proxy.client.route;

import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class MessageQueueWrapper {
    public static final MessageQueueWrapper EMPTY_CACHED_QUEUE = new MessageQueueWrapper("", new TopicRouteData());

    private final SelectableMessageQueue read;
    private final SelectableMessageQueue write;
    private final TopicRouteWrapper topicRouteWrapper;

    public MessageQueueWrapper(String topic, TopicRouteData topicRouteData) {
        this.topicRouteWrapper = new TopicRouteWrapper(topicRouteData, topic);

        this.read = new SelectableMessageQueue(topicRouteWrapper, true);
        this.write = new SelectableMessageQueue(topicRouteWrapper, false);
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteWrapper.getTopicRouteData();
    }

    public String getTopicName() {
        return topicRouteWrapper.getTopicName();
    }

    public boolean isEmptyCachedQueue() {
        return this == EMPTY_CACHED_QUEUE;
    }

    @Override
    public String toString() {
        return "MessageQueueWrapper{" +
            "read=" + read +
            ", write=" + write +
            ", topicRouteWrapper=" + topicRouteWrapper +
            '}';
    }
}