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

package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;

public class PollingHeader {
    private final String consumerGroup;
    private final String topic;
    private final int queueId;
    private final long bornTime;
    private final long pollTime;

    public PollingHeader(PopMessageRequestHeader requestHeader) {
        this.consumerGroup = requestHeader.getConsumerGroup();
        this.topic = requestHeader.getTopic();
        this.queueId = requestHeader.getQueueId();
        this.bornTime = requestHeader.getBornTime();
        this.pollTime = requestHeader.getPollTime();
    }

    public PollingHeader(NotificationRequestHeader requestHeader) {
        this.consumerGroup = requestHeader.getConsumerGroup();
        this.topic = requestHeader.getTopic();
        this.queueId = requestHeader.getQueueId();
        this.bornTime = requestHeader.getBornTime();
        this.pollTime = requestHeader.getPollTime();
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getBornTime() {
        return bornTime;
    }

    public long getPollTime() {
        return pollTime;
    }
}
