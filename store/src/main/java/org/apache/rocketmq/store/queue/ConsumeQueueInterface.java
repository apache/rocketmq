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

package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageFilter;

public interface ConsumeQueueInterface {
    /**
     * Get the topic name
     * @return the topic this cq belongs to.
     */
    String getTopic();

    /**
     * Get queue id
     * @return the queue id this cq belongs to.
     */
    int getQueueId();

    /**
     * Get the units from the start offset.
     *
     * @param startIndex start index
     * @return the unit iterateFrom
     */
    ReferredIterator<CqUnit> iterateFrom(long startIndex);

    /**
     * Get cq unit at specified index
     * @param index index
     * @return the cq unit at index
     */
    CqUnit get(long index);

    /**
     * Get earliest cq unit
     * @return earliest cq unit
     */
    CqUnit getEarliestUnit();

    /**
     * Get last cq unit
     * @return last cq unit
     */
    CqUnit getLatestUnit();

    /**
     * Get last commit log offset
     * @return last commit log offset
     */
    long getLastOffset();

    /**
     * Get min offset(index) in queue
     * @return the min offset(index) in queue
     */
    long getMinOffsetInQueue();

    /**
     * Get max offset(index) in queue
     * @return the max offset(index) in queue
     */
    long getMaxOffsetInQueue();

    /**
     * Get total message count
     * @return total message count
     */
    long getMessageTotalInQueue();

    /**
     * Get the message whose timestamp is the smallest, greater than or equal to the given time.
     * @param timestamp timestamp
     * @return the offset(index)
     */
    long getOffsetInQueueByTime(final long timestamp);

    /**
     * The max physical offset of commitlog has been dispatched to this queue.
     * It should be exclusive.
     *
     * @return the max physical offset point to commitlog
     */
    long getMaxPhysicOffset();

    /**
     * Usually, the cq files are not exactly consistent with the commitlog, there maybe some redundant data in the first
     * cq file.
     *
     * @return the minimal effective pos of the cq file.
     */
    long getMinLogicOffset();

    /**
     * Get cq type
     * @return cq type
     */
    CQType getCQType();

    /**
     * Gets the occupied size of CQ file on disk
     * @return total size
     */
    long getTotalSize();

    /**
     * Get the unit size of this CQ which is different in different CQ impl
     * @return cq unit size
     */
    int getUnitSize();

    /**
     * Correct min offset by min commit log offset.
     * @param minCommitLogOffset min commit log offset
     */
    void correctMinOffset(long minCommitLogOffset);

    /**
     * Do dispatch.
     * @param request the request containing dispatch information.
     */
    void putMessagePositionInfoWrapper(DispatchRequest request);

    /**
     * Assign queue offset.
     * @param queueOffsetAssigner the delegated queue offset assigner
     * @param msg message itself
     * @param messageNum message number
     */
    void assignQueueOffset(QueueOffsetAssigner queueOffsetAssigner, MessageExtBrokerInner msg, short messageNum);

    /**
     * Estimate number of records matching given filter.
     *
     * @param from Lower boundary, inclusive.
     * @param to Upper boundary, inclusive.
     * @param filter Specified filter criteria
     * @return Number of matching records.
     */
    long estimateMessageCount(long from, long to, MessageFilter filter);
}
