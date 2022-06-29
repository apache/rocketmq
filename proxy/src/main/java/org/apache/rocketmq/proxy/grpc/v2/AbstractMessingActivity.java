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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public abstract class AbstractMessingActivity {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final MessagingProcessor messagingProcessor;
    protected final GrpcClientSettingsManager grpcClientSettingsManager;
    protected final GrpcChannelManager grpcChannelManager;

    protected static final Map<String, Pattern> CHECK_PATTERN_CACHE = new ConcurrentHashMap<>();

    public AbstractMessingActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        this.messagingProcessor = messagingProcessor;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.grpcChannelManager = grpcChannelManager;
    }

    protected Pattern getPattern(String regex) {
        return CHECK_PATTERN_CACHE.compute(regex, (regexKey, oldPattern) -> {
            try {
                return Pattern.compile(regex);
            } catch (Exception e) {
                log.error("create check pattern failed. regex:{}", regex, e);
                return oldPattern;
            }
        });
    }

    protected void validateTopic(Resource topic) {
        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
        if (StringUtils.isBlank(topicName)) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC, "topic name cannot be empty");
        }
        if (TopicValidator.isSystemTopic(topicName)) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC, "cannot access system topic");
        }
        String regex = ConfigurationManager.getProxyConfig().getTopicNameCheckRegex();
        if (StringUtils.isBlank(regex)) {
            return;
        }
        Pattern pattern = getPattern(regex);
        if (pattern == null) {
            throw new GrpcProxyException(Code.INTERNAL_SERVER_ERROR, "get topic name check pattern failed");
        }
        if (!pattern.matcher(topicName).matches()) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC, "the format of topic is not correct");
        }
    }

    protected void validateConsumerGroup(Resource consumerGroup) {
        String consumerGroupName = GrpcConverter.wrapResourceWithNamespace(consumerGroup);
        if (StringUtils.isBlank(consumerGroupName)) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, "consumer group cannot be empty");
        }
        if (MixAll.isSysConsumerGroup(consumerGroupName)) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, "cannot use system consumer group");
        }
        String regex = ConfigurationManager.getProxyConfig().getConsumerGroupNameCheckRegex();
        if (StringUtils.isBlank(regex)) {
            return;
        }
        Pattern pattern = getPattern(regex);
        if (pattern == null) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, "get consumer group check pattern failed");
        }
        if (!pattern.matcher(consumerGroupName).matches()) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, "the format of consumer group is not correct");
        }
    }

    protected void validateTopicAndConsumerGroup(Resource topic, Resource consumerGroup) {
        validateTopic(topic);
        validateConsumerGroup(consumerGroup);
    }
}
