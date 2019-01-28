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
package org.apache.rocketmq.snode.service;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface EnodeService {
    void sendHearbeat(RemotingCommand remotingCommand);

    CompletableFuture<RemotingCommand> sendMessage(final String enodeName, final RemotingCommand request);

    CompletableFuture<RemotingCommand> pullMessage(final String enodeName, final RemotingCommand request);

    RemotingCommand creatTopic(String enodeName,
        TopicConfig topicConfig) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

    void updateEnodeAddress(String clusterName) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException;

    boolean persistSubscriptionGroupConfig(SubscriptionGroupConfig subscriptionGroupConfig);

    void persistOffset(String enodeName, String groupName, String topic, int queueId, long offset);

    RemotingCommand loadOffset(String enodeName, String consumerGroup, String topic,
        int queueId) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException;

    RemotingCommand getMaxOffsetInQueue(String enodeName,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException;

    RemotingCommand getMinOffsetInQueue(String enodeName, String topic,
        int queueId) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException;

    RemotingCommand getOffsetByTimestamp(String enodeName,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;
}
