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
package org.apache.rocketmq.proxy.service.route;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.message.MessageQueue;

@FunctionalInterface
public interface MessageQueuePenalizer <Q extends MessageQueue> {

    /**
     * Returns the penalty value for the given MessageQueue; lower is better.
     */
    int penaltyOf(Q messageQueue);

    /**
     * Aggregates penalties from multiple penalizers for the same MessageQueue (by summing them up).
     */
    static <Q extends MessageQueue> int evaluatePenalty(Q messageQueue, List<MessageQueuePenalizer<Q>> penalizers) {
        Objects.requireNonNull(messageQueue, "messageQueue");
        if (penalizers == null || penalizers.isEmpty()) {
            return 0;
        }
        return penalizers.stream()
            .mapToInt(p -> p.penaltyOf(messageQueue))
            .sum();
    }

    /**
     * Selects the MessageQueue with the smallest total penalty.
     * Returns null if queues is null/empty.
     */
    static <Q extends MessageQueue> Pair<Q, Integer> selectLeastPenalty(List<Q> queues,
        List<MessageQueuePenalizer<Q>> penalizers, AtomicInteger startIndex) {
        if (queues == null || queues.isEmpty()) {
            return null;
        }
        Q bestQueue = null;
        int bestPenalty = Integer.MAX_VALUE;

        for (int i = 0; i < queues.size(); i++) {
            int index = Math.floorMod(startIndex.getAndIncrement(), queues.size());
            Q messageQueue = queues.get(index);
            int penalty = evaluatePenalty(messageQueue, penalizers);

            // Short-circuit: cannot do better than 0
            if (penalty <= 0) {
                return Pair.of(messageQueue, penalty);
            }

            if (penalty < bestPenalty) {
                bestPenalty = penalty;
                bestQueue = messageQueue;
            }
        }
        return Pair.of(bestQueue,  bestPenalty);
    }

    /**
     * Selects the MessageQueue with the smallest total penalty.
     * Returns null if queuesWithPriority is null/empty.
     */
    static <Q extends MessageQueue> Pair<Q, Integer> selectLeastPenaltyWithPriority(List<List<Q>> queuesWithPriority,
        List<MessageQueuePenalizer<Q>> penalizers, AtomicInteger startIndex) {
        if (queuesWithPriority == null || queuesWithPriority.isEmpty()) {
            return null;
        }
        Q bestQueue = null;
        int bestPenalty = Integer.MAX_VALUE;
        for (List<Q> queues : queuesWithPriority) {
            Pair<Q, Integer> queueAndPenalty = selectLeastPenalty(queues, penalizers, startIndex);
            int penalty =  queueAndPenalty.getRight();
            if (queueAndPenalty.getRight() <= 0) {
                return queueAndPenalty;
            }
            if (penalty < bestPenalty) {
                bestPenalty = penalty;
                bestQueue = queueAndPenalty.getLeft();
            }
        }
        return Pair.of(bestQueue,  bestPenalty);
    }
}