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

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.MessageQueue;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcConverterTest {
    @Test
    public void testBuildMessageQueue() {
        String topic = "topic";
        String brokerName = "brokerName";
        int queueId = 1;
        MessageExt messageExt = new MessageExt();
        messageExt.setQueueId(queueId);
        messageExt.setTopic(topic);

        MessageQueue messageQueue = GrpcConverter.getInstance().buildMessageQueue(messageExt, brokerName);
        assertThat(messageQueue.getTopic().getName()).isEqualTo(topic);
        assertThat(messageQueue.getBroker().getName()).isEqualTo(brokerName);
        assertThat(messageQueue.getId()).isEqualTo(queueId);
    }
}