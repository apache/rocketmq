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
package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.Resource;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcValidator;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public abstract class AbstractMessingActivity {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final MessagingProcessor messagingProcessor;
    protected final GrpcClientSettingsManager grpcClientSettingsManager;
    protected final GrpcChannelManager grpcChannelManager;

    public AbstractMessingActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        this.messagingProcessor = messagingProcessor;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.grpcChannelManager = grpcChannelManager;
    }

    protected void validateTopic(Resource topic) {
        GrpcValidator.getInstance().validateTopic(topic);
    }

    protected void validateConsumerGroup(Resource consumerGroup) {
        GrpcValidator.getInstance().validateConsumerGroup(consumerGroup);
    }

    protected void validateTopicAndConsumerGroup(Resource topic, Resource consumerGroup) {
        GrpcValidator.getInstance().validateTopicAndConsumerGroup(topic, consumerGroup);
    }

    protected void validateInvisibleTime(long invisibleTime) {
        GrpcValidator.getInstance().validateInvisibleTime(invisibleTime);
    }

    protected void validateInvisibleTime(long invisibleTime, long minInvisibleTime) {
        GrpcValidator.getInstance().validateInvisibleTime(invisibleTime, minInvisibleTime);
    }
}
