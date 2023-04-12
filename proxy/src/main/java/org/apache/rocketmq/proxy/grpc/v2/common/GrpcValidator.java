/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Resource;
import com.google.common.base.CharMatcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

public class GrpcValidator {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    protected static volatile GrpcValidator instance;

    public static GrpcValidator getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new GrpcValidator();
                }
            }
        }
        return instance;
    }

    public void validateTopic(Resource topic) {
        validateTopic(GrpcConverter.getInstance().wrapResourceWithNamespace(topic));
    }

    public void validateTopic(String topicName) {
        try {
            Validators.checkTopic(topicName);
        } catch (MQClientException mqClientException) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC, mqClientException.getErrorMessage());
        }
        if (TopicValidator.isSystemTopic(topicName)) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC, "cannot access system topic");
        }
    }

    public void validateConsumerGroup(Resource consumerGroup) {
        validateConsumerGroup(GrpcConverter.getInstance().wrapResourceWithNamespace(consumerGroup));
    }

    public void validateConsumerGroup(String consumerGroupName) {
        try {
            Validators.checkGroup(consumerGroupName);
        } catch (MQClientException mqClientException) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, mqClientException.getErrorMessage());
        }
        if (MixAll.isSysConsumerGroup(consumerGroupName)) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, "cannot use system consumer group");
        }
    }

    public void validateTopicAndConsumerGroup(Resource topic, Resource consumerGroup) {
        validateTopic(topic);
        validateConsumerGroup(consumerGroup);
    }

    public void validateInvisibleTime(long invisibleTime) {
        validateInvisibleTime(invisibleTime, 0);
    }

    public void validateInvisibleTime(long invisibleTime, long minInvisibleTime) {
        if (invisibleTime < minInvisibleTime) {
            throw new GrpcProxyException(Code.ILLEGAL_INVISIBLE_TIME, "the invisibleTime is too small. min is " + minInvisibleTime);
        }
        long maxInvisibleTime = ConfigurationManager.getProxyConfig().getMaxInvisibleTimeMills();
        if (maxInvisibleTime <= 0) {
            return;
        }
        if (invisibleTime > maxInvisibleTime) {
            throw new GrpcProxyException(Code.ILLEGAL_INVISIBLE_TIME, "the invisibleTime is too large. max is " + maxInvisibleTime);
        }
    }

    public void validateTag(String tag) {
        if (StringUtils.isNotEmpty(tag)) {
            if (StringUtils.isBlank(tag)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_TAG, "tag cannot be the char sequence of whitespace");
            }
            if (tag.contains("|")) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_TAG, "tag cannot contain '|'");
            }
            if (containControlCharacter(tag)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_TAG, "tag cannot contain control character");
            }
        }
    }

    public boolean containControlCharacter(String data) {
        for (int i = 0; i < data.length(); i++) {
            if (CharMatcher.javaIsoControl().matches(data.charAt(i))) {
                return true;
            }
        }
        return false;
    }
}
