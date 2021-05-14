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

import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A MessageListenerConcurrently object is used to receive
 * asynchronously delivered messages periodically and concurrently.
 * For example, the top 10 orders can get an extra laptop,
 * the top 10-30 orders can get an extra tablet,
 * the top 30-100 orders can get an extra mobile phone,
 * and there is no extra reward for placing orders after 100.
 * We call the interval from 1-10 as "stage one",
 * the interval from 10-30 as "stage two",
 * the interval from 30-100 as "stage three",
 * and the interval from 100+ as "stage" four".
 * From an overall point of view, stages one, two, three, and four
 * are in order; from a partial point of view, such as stage one,
 * the internal stage is out of order.
 * The rewards are the same, why not consume concurrently?
 */
public interface MessageListenerPeriodicConcurrently extends MessageListener {
    /**
     * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if
     * consumption failure
     *
     * @param msgs msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,
                                        final ConsumeOrderlyContext context,
                                        final int stageIndex);

    /**
     * If returns empty collection, {@link MessageListenerPeriodicConcurrently}
     * will degenerate into {@link MessageListenerConcurrently};
     * If returns a collection whose elements are all 1,
     * {@link MessageListenerPeriodicConcurrently} will temporarily
     * evolve into {@link MessageListenerOrderly};
     */
    default List<Integer> getStageDefinitions() {
        return new ArrayList<>();
    }

    /**
     * can be used to reset the current stage by CAS
     */
    default void resetCurrentStageIfNeed(final AtomicInteger currentStage) {
    }

    /**
     * if {@code System.currentTimeMillis()}>={@code #getConsumeFromTimeStamp},
     * then start consume message, this method will be called every time when
     * pulled message successfully.
     */
    default long getConsumeFromTimeStamp() {
        return 0;
    }
}
