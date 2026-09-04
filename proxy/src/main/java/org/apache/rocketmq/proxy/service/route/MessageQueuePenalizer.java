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
public interface MessageQueuePenalizer<Q extends MessageQueue> {

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
        int sum = 0;
        for (MessageQueuePenalizer<Q> p : penalizers) {
            sum += p.penaltyOf(messageQueue);
        }
        return sum;
    }

    /**
     * Selects the queue with the lowest evaluated penalty from the given queue list.
     *
     * <p>The method iterates through all queues exactly once, but starts from a rotating index
     * derived from {@code startIndex} (round-robin) to avoid always scanning from position 0 .</p>
     *
     * <p>For each queue, it computes a penalty via {@link #evaluatePenalty} using
     * the provided {@code penalizers}. The queue with the smallest penalty is selected.</p>
     *
     * <p>Short-circuit rule: if any queue has a {@code penalty<= 0}, it is returned immediately,
     * since no better result than 0 is expected.</p>
     *
     * @param queues candidate queues to select from
     * @param penalizers penalty evaluators applied to each queue
     * @param startIndex atomic counter used to determine the rotating start position (round-robin)
     * @param <Q> queue type
     * @return a {@code Pair} of (selected queue, penalty), or {@code null} if {@code queues} is null/empty
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
     * Selects a queue with the lowest computed penalty from multiple priority groups.
     *
     * <p>The input {@code queuesWithPriority} is a list of queue groups ordered by priority.
     * For each priority group, this method delegates to {@link #selectLeastPenalty} to pick the best queue
     * within that group and obtain its penalty.</p>
     *
     * <p>Short-circuit rule: if any priority group yields a queue whose {@code penalty <= 0},
     * that result is returned immediately.</p>
     *
     * <p>Otherwise, it returns the queue with the smallest positive penalty among all groups.
     * If multiple groups produce the same minimum penalty, the first encountered one wins.</p>
     *
     * @param queuesWithPriority priority-ordered groups of queues; each inner list represents one priority level
     * @param penalizers penalty calculators used by {@code selectLeastPenalty} to score queues
     * @param startIndex round-robin start index forwarded to {@code selectLeastPenalty} to reduce contention/hotspots
     * @param <Q> queue type
     * @return a {@code Pair} of (selected queue, penalty), or {@code null} if {@code queuesWithPriority} is null/empty
     */
    static <Q extends MessageQueue> Pair<Q, Integer> selectLeastPenaltyWithPriority(List<List<Q>> queuesWithPriority,
        List<MessageQueuePenalizer<Q>> penalizers, AtomicInteger startIndex) {
        if (queuesWithPriority == null || queuesWithPriority.isEmpty()) {
            return null;
        }
        if (queuesWithPriority.size() == 1) {
            return selectLeastPenalty(queuesWithPriority.get(0), penalizers, startIndex);
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