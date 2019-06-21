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
package org.apache.rocketmq.client.consumer;

import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.LiteMQPullConsumerImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;

public class DefaultLiteMQPullConsumer extends DefaultMQPullConsumer implements LiteMQPullConsumer {
    private LiteMQPullConsumerImpl liteMQPullConsumer;

    public DefaultLiteMQPullConsumer(String consumerGroup, RPCHook rpcHook) {
        this.liteMQPullConsumer = new LiteMQPullConsumerImpl(this, rpcHook);
    }

    @Override public void subscribe(String topic, String subExpression) throws MQClientException{
        this.liteMQPullConsumer.subscribe(topic, subExpression);
    }

    @Override public void unsubscribe(String topic) {
    }

    @Override public List<MessageExt> poll() {
        return poll(this.getConsumerPullTimeoutMillis());
    }

    @Override public List<MessageExt> poll(long timeout) {
        return liteMQPullConsumer.poll(timeout);
    }

    @Override public void seek(MessageQueue messageQueue, long offset) throws MQClientException {

    }

    @Override public void pause(Collection<MessageQueue> messageQueueCollection) {

    }

    @Override public void resume(Collection<MessageQueue> partitions) {

    }

    @Override public void commitSync() {

    }
}
