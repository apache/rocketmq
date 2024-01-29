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
package org.apache.rocketmq.common;

public class KeyBuilder {
    public static final int POP_ORDER_REVIVE_QUEUE = 999;
    private static final char POP_RETRY_SEPARATOR_V1 = '_';
    private static final char POP_RETRY_SEPARATOR_V2 = '+';
    private static final String POP_RETRY_REGEX_SEPARATOR_V2 = "\\+";

    public static String buildPopRetryTopic(String topic, String cid, boolean enableRetryV2) {
        if (enableRetryV2) {
            return buildPopRetryTopicV2(topic, cid);
        }
        return buildPopRetryTopicV1(topic, cid);
    }

    public static String buildPopRetryTopic(String topic, String cid) {
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V1 + topic;
    }

    public static String buildPopRetryTopicV2(String topic, String cid) {
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V2 + topic;
    }

    public static String buildPopRetryTopicV1(String topic, String cid) {
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V1 + topic;
    }

    public static String parseNormalTopic(String topic, String cid) {
        if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V2)) {
                return topic.substring((MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V2).length());
            }
            return topic.substring((MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + POP_RETRY_SEPARATOR_V1).length());
        } else {
            return topic;
        }
    }

    public static String parseNormalTopic(String retryTopic) {
        if (isPopRetryTopicV2(retryTopic)) {
            String[] result = retryTopic.split(POP_RETRY_REGEX_SEPARATOR_V2);
            if (result.length == 2) {
                return result[1];
            }
        }
        return retryTopic;
    }

    public static String parseGroup(String retryTopic) {
        if (isPopRetryTopicV2(retryTopic)) {
            String[] result = retryTopic.split(POP_RETRY_REGEX_SEPARATOR_V2);
            if (result.length == 2) {
                return result[0].substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            }
        }
        return retryTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
    }

    public static String buildPollingKey(String topic, String cid, int queueId) {
        return topic + PopAckConstants.SPLIT + cid + PopAckConstants.SPLIT + queueId;
    }

    public static boolean isPopRetryTopicV2(String retryTopic) {
        return retryTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && retryTopic.contains(String.valueOf(POP_RETRY_SEPARATOR_V2));
    }
}
