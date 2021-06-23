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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;

public class SendResultForLogicalQueue extends SendResult {
    private final String origBrokerName;
    private final int origQueueId;

    public SendResultForLogicalQueue(SendResult sendResult, int logicalQueueIdx) {
        super(sendResult.getSendStatus(), sendResult.getMsgId(), sendResult.getOffsetMsgId(), new MessageQueue(sendResult.getMessageQueue().getTopic(), MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, logicalQueueIdx), sendResult.getQueueOffset());
        this.origBrokerName = sendResult.getMessageQueue().getBrokerName();
        this.origQueueId = sendResult.getMessageQueue().getQueueId();
    }

    public String getOrigBrokerName() {
        return origBrokerName;
    }

    public int getOrigQueueId() {
        return origQueueId;
    }

    @Override public String toString() {
        return "SendResultForLogicalQueue{" +
            "origBrokerName='" + origBrokerName + '\'' +
            ", origQueueId=" + origQueueId +
            "} " + super.toString();
    }
}
