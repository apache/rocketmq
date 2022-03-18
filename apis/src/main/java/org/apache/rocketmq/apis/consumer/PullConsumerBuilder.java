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

package org.apache.rocketmq.apis.consumer;

import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.exception.ClientException;

import java.time.Duration;

public interface PullConsumerBuilder {
    /**
     * Set the client configuration for pull consumer.
     *
     * @param clientConfiguration client's configuration.
     * @return the pull consumer builder instance.
     */
    PullConsumerBuilder setClientConfiguration(ClientConfiguration clientConfiguration);

    /**
     * Set the load balancing group for consumer.
     *
     * @param consumerGroup consumer load balancing group.
     * @return the consumer builder instance.
     */
    PullConsumerBuilder setConsumerGroup(String consumerGroup);

    /**
     * Enable manual messageQueue assignment consumption mode.
     * The default mode is subscription mode which manage the rebalance operation triggered when group membership or cluster and topic metadata change.
     * When pull consumer manual queue assignment mode, must invoke assign method before pull message.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder enableManualQueueAssignment();

    /**
     * Set the max await time when receive message from server.
     * The simple consumer will hold this long-polling receive requests until  a message is returned or a timeout occurs.
     * @param awaitDuration The maximum time to block when no message available.
     * @return the consumer builder instance.
     */
    PushConsumerBuilder setAwaitDuration(Duration awaitDuration);

    /**
     * Finalize the build of the {@link PullConsumer} instance and start.
     *
     * <p>This method will block until pull consumer starts successfully.
     *
     * @return the pull consumer instance.
     */
    PullConsumer build() throws ClientException;
}
