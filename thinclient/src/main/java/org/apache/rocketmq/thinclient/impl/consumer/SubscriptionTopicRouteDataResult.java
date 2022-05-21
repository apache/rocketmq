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

package org.apache.rocketmq.thinclient.impl.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.thinclient.exception.ResourceNotFoundException;
import org.apache.rocketmq.thinclient.misc.Utilities;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;
import org.apache.rocketmq.thinclient.route.TopicRouteDataResult;

@Immutable
public class SubscriptionTopicRouteDataResult {
    private final AtomicInteger messageQueueIndex;

    private final Status status;

    private final ImmutableList<MessageQueueImpl> messageQueues;

    public SubscriptionTopicRouteDataResult(TopicRouteDataResult topicRouteDataResult) {
        this.messageQueueIndex = new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        this.status = topicRouteDataResult.getStatus();
        final ImmutableList.Builder<MessageQueueImpl> builder = ImmutableList.builder();
        if (Code.OK != status.getCode()) {
            this.messageQueues = builder.build();
            return;
        }
        for (MessageQueueImpl messageQueue : topicRouteDataResult.getTopicRouteData().getMessageQueues()) {
            if (!messageQueue.getPermission().isReadable() ||
                Utilities.MASTER_BROKER_ID != messageQueue.getBroker().getId()) {
                continue;
            }
            builder.add(messageQueue);
        }
        this.messageQueues = builder.build();
    }

    public MessageQueueImpl takeMessageQueue() throws ClientException {
        final Code code = status.getCode();
        if (!Code.OK.equals(code)) {
            throw new ClientException(code.getNumber(), status.getMessage());
        }
        if (messageQueues.isEmpty()) {
            // Should never reach here.
            throw new ResourceNotFoundException("Failed to take message queue due to readable message queue doesn't exist");
        }
        final int next = messageQueueIndex.getAndIncrement();
        return messageQueues.get(IntMath.mod(next, messageQueues.size()));
    }
}
