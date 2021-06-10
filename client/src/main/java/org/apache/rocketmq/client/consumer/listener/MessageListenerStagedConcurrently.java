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
package org.apache.rocketmq.client.consumer.listener;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * A MessageListenerConcurrently object is used to receive asynchronously delivered messages periodically and
 * concurrently. For example, the top 10 orders can get an extra laptop, the top 10-30 orders can get an extra tablet,
 * the top 30-100 orders can get an extra mobile phone, and there is no extra reward for placing orders after 100. We
 * call the interval from 1-10 as "stage one", the interval from 10-30 as "stage two", the interval from 30-100 as
 * "stage three", and the interval from 100+ as "stage" four". From an overall point of view, stages one, two, three,
 * and four are in order; from a partial point of view, such as stage one, the internal stage is out of order. The
 * rewards are the same, why not consume concurrently?
 */
public interface MessageListenerStagedConcurrently extends MessageListener {
    /**
     * It is not recommend to throw exception,rather than returning ConsumeOrderlyStatus#SUSPEND_CURRENT_QUEUE_A_MOMENT
     * if consumption failure
     *
     * @param msgs msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,
        final ConsumeStagedConcurrentlyContext context);

    /**
     * The Map structure here is mainly for a consumer to support multiple phased concurrency strategies at the same
     * time.The following are specific instructions:
     * <p>
     * key:user can customize the strategy id. For example, "activityId + shopId".
     * <p>
     * value: If returns empty list or null, {@link MessageListenerStagedConcurrently} will degenerate into {@link
     * MessageListenerConcurrently}; If returns a collection whose elements are all 1, {@link
     * MessageListenerStagedConcurrently} will temporarily evolve into {@link MessageListenerOrderly};
     *
     * @see MessageListenerStagedConcurrently#computeStrategy(org.apache.rocketmq.common.message.MessageExt)
     */
    Map<String/*strategyId*/, List<Integer>/*StageDefinition*/> getStageDefinitionStrategies();

    /**
     * Calculate to which strategy is used for message ,if return null or the strategy not found, the message of this
     * method input will be directly concurrently consume, otherwise the message will be phased concurrently consume.
     *
     * @return strategyId
     * @see MessageListenerStagedConcurrently#getStageDefinitionStrategies()
     */
    String computeStrategy(MessageExt message);

    /**
     * can be used to reset the current stage by CAS
     */
    default void resetCurrentStageOffsetIfNeed(final String topic, String strategyId,
        final AtomicInteger currentStageOffset) {
    }
}
