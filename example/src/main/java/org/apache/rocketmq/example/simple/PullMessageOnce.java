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

package org.apache.rocketmq.example.simple;

import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class PullMessageOnce {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pullTopicTest_consumerGroup");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();
        Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues("PullTopicTest", "*");
        for (MessageQueue messageQueue : messageQueues) {
            long offset = consumer.fetchConsumeOffset(messageQueue, true);
            if (offset < 0) {
                continue;
            }
            PullResult result = consumer.pull(messageQueue, "*", offset, 1, 3000);
            long i = result.getNextBeginOffset();
            if (result.getPullStatus() == PullStatus.FOUND) {
                consumer.updateConsumeOffset(messageQueue, i);
            }
        }
        // wait schedule update offset to broker
        Thread.sleep(10000);
        consumer.shutdown();
    }
}
