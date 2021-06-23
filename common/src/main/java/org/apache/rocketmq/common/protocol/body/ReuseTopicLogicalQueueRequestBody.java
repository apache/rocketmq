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
package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ReuseTopicLogicalQueueRequestBody extends RemotingSerializable {
    private String topic;
    private int queueId;
    private int logicalQueueIndex;
    private MessageQueueRouteState messageQueueRouteState;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getLogicalQueueIndex() {
        return logicalQueueIndex;
    }

    public void setLogicalQueueIndex(int logicalQueueIndex) {
        this.logicalQueueIndex = logicalQueueIndex;
    }

    public void setMessageQueueRouteState(MessageQueueRouteState messageQueueRouteState) {
        this.messageQueueRouteState = messageQueueRouteState;
    }

    public MessageQueueRouteState getMessageQueueRouteState() {
        return messageQueueRouteState;
    }
}
