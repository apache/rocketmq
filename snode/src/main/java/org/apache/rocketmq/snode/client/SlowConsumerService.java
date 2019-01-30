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
package org.apache.rocketmq.snode.client;

import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface SlowConsumerService {

    /**
     * Check whether this consumer is slow consumer, slow consumer means this consumer's acked offset is far behind
     * current offset.
     *
     * @param currentOffset Current offset in consumer queue.
     * @param topic
     * @param queueId
     * @param consumerGroup
     * @param enodeName
     * @return If this consumer is slow consumer, return true, otherwise false.
     */
    boolean isSlowConsumer(long currentOffset, String topic, int queueId, String consumerGroup, String enodeName);

    /**
     * When a consumer is checked as slow consumer, this method will be invoked to do next action.
     *
     * @param pushMessage The message will be pushed to consumer.
     * @param remotingChannel Consumer channel.
     */
    void slowConsumerResolve(RemotingCommand pushMessage, RemotingChannel remotingChannel);
}
