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

package org.apache.rocketmq.remoting.protocol.body;

import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class SetMessageRequestModeRequestBody extends RemotingSerializable {

    private String topic;

    private String consumerGroup;

    private MessageRequestMode mode = MessageRequestMode.PULL;

    /*
    consumer working in pop mode could share the MessageQueues assigned to the N (N = popShareQueueNum) consumers following it in the cid list
     */
    private int popShareQueueNum = 0;

    public SetMessageRequestModeRequestBody() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageRequestMode getMode() {
        return mode;
    }

    public void setMode(MessageRequestMode mode) {
        this.mode = mode;
    }

    public int getPopShareQueueNum() {
        return popShareQueueNum;
    }

    public void setPopShareQueueNum(int popShareQueueNum) {
        this.popShareQueueNum = popShareQueueNum;
    }
}
