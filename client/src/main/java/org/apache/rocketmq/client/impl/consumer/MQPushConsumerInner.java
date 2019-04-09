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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.MQRealPushConsumer;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.common.exception.MQBrokerException;
import org.apache.rocketmq.common.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Push Consumer inner interface
 */
public interface MQPushConsumerInner extends MQConsumerInner {

    MQRealPushConsumer getDefaultMQPushConsumer();

    OffsetStore getOffsetStore();

    boolean isConsumeOrderly();

    MQClientInstance getmQClientFactory();

    boolean resumePullRequest(String consumerGroup, String topic, String brokerName, int queueID);

    void executePullRequestImmediately(final PullRequest pullRequest);

    RebalanceImpl getRebalanceImpl();

    ConsumerStatsManager getConsumerStatsManager();

    void sendMessageBack(MessageExt msg, int delayLevel,
        final String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    boolean hasHook();

    void registerConsumeMessageHook(final ConsumeMessageHook hook);

    void executeHookBefore(final ConsumeMessageContext context);

    void executeHookAfter(final ConsumeMessageContext context);

    void pullMessage(final PullRequest pullRequest);
}
