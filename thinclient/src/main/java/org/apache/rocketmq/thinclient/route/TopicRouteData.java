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

package org.apache.rocketmq.thinclient.route;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import org.apache.commons.lang3.RandomUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.thinclient.exception.ResourceNotFoundException;
import org.apache.rocketmq.thinclient.misc.Utilities;

public class TopicRouteData {
    private final AtomicInteger index;
    /**
     * Message queues of topic route.
     */
    private final ImmutableList<MessageQueueImpl> messageQueueImpls;

    /**
     * Construct topic route by message queues.
     *
     * @param messageQueues message queue list, should never be empty.
     */
    public TopicRouteData(List<apache.rocketmq.v2.MessageQueue> messageQueues) {
        this.index = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        final ImmutableList.Builder<MessageQueueImpl> builder = ImmutableList.builder();
        for (apache.rocketmq.v2.MessageQueue messageQueue : messageQueues) {
            builder.add(new MessageQueueImpl(messageQueue));
        }
        this.messageQueueImpls = builder.build();
    }

    public Set<Endpoints> getTotalEndpoints() {
        Set<Endpoints> endpointsSet = new HashSet<>();
        for (MessageQueueImpl messageQueueImpl : messageQueueImpls) {
            endpointsSet.add(messageQueueImpl.getBroker().getEndpoints());
        }
        return endpointsSet;
    }

    public List<MessageQueueImpl> getMessageQueues() {
        return this.messageQueueImpls;
    }

    public Endpoints pickEndpointsToQueryAssignments() throws ResourceNotFoundException {
        int nextIndex = index.getAndIncrement();
        for (int i = 0; i < messageQueueImpls.size(); i++) {
            final MessageQueueImpl messageQueueImpl = messageQueueImpls.get(IntMath.mod(nextIndex++, messageQueueImpls.size()));
            final Broker broker = messageQueueImpl.getBroker();
            if (Utilities.MASTER_BROKER_ID != broker.getId()) {
                continue;
            }
            if (Permission.NONE.equals(messageQueueImpl.getPermission())) {
                continue;
            }
            return broker.getEndpoints();
        }
        throw new ResourceNotFoundException("Failed to pick endpoints to query assignment");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicRouteData that = (TopicRouteData) o;
        return Objects.equal(messageQueueImpls, that.messageQueueImpls);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageQueueImpls);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageQueueImpls", messageQueueImpls)
            .toString();
    }
}
