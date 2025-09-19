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
package org.apache.rocketmq.common.topic;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;

public class TopicValidator {

    public static final String AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102"; // Will be created at broker when isAutoCreateTopicEnable
    public static final String RMQ_SYS_SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    public static final String RMQ_SYS_BENCHMARK_TOPIC = "BenchmarkTest";
    public static final String RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";
    public static final String RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC";
    public static final String RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    public static final String RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "TRANS_CHECK_MAX_TIME_TOPIC";
    public static final String RMQ_SYS_SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
    public static final String RMQ_SYS_OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT";
    public static final String RMQ_SYS_ROCKSDB_OFFSET_TOPIC = "CHECKPOINT_TOPIC";
    public static final String RMQ_SYS_TOPIC_CONFIG_SYNC = "RMQ_SYS_TOPIC_CONFIG_SYNC";
    public static final String RMQ_SYS_CONSUMER_OFFSET_SYNC = "RMQ_SYS_CONSUMER_OFFSET_SYNC";
    public static final String RMQ_SYS_DELAY_OFFSET_SYNC = "RMQ_SYS_DELAY_OFFSET_SYNC";
    public static final String RMQ_SYS_SUBSCRIPTION_GROUP_SYNC = "RMQ_SYS_SUBSCRIPTION_GROUP_SYNC";
    public static final String RMQ_SYS_MESSAGE_MODE_SYNC = "RMQ_SYS_MESSAGE_MODE_SYNC";
    public static final String RMQ_SYS_TIMER_METRICS_SYNC = "RMQ_SYS_TIMER_METRICS_SYNC";
    public static final String SYSTEM_TOPIC_PREFIX = "rmq_sys_";
    public static final String SYNC_BROKER_MEMBER_GROUP_PREFIX = SYSTEM_TOPIC_PREFIX + "SYNC_BROKER_MEMBER_";

    public static final boolean[] VALID_CHAR_BIT_MAP = new boolean[128];
    private static final int TOPIC_MAX_LENGTH = 127;
    /*
     * Group name max length is 120, for it will be used to make up retry and DLQ topic,
     * like pull retry: %RETRY%group_topic and pop retry: %RETRY%group_topic.
     */
    private static final int GROUP_MAX_LENGTH = 120;
    private static final int RETRY_OR_DLQ_TOPIC_MAX_LENGTH = 255;

    private static final Set<String> SYSTEM_TOPIC_SET = new HashSet<>();

    /**
     * Topic set which client can not send msg!
     */
    private static final Set<String> NOT_ALLOWED_SEND_TOPIC_SET = new HashSet<>();

    static {
        SYSTEM_TOPIC_SET.add(AUTO_CREATE_TOPIC_KEY_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_SCHEDULE_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_BENCHMARK_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_HALF_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRACE_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_OP_HALF_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_SELF_TEST_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_OFFSET_MOVED_EVENT);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_ROCKSDB_OFFSET_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TOPIC_CONFIG_SYNC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_CONSUMER_OFFSET_SYNC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_DELAY_OFFSET_SYNC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_SUBSCRIPTION_GROUP_SYNC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_MESSAGE_MODE_SYNC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TIMER_METRICS_SYNC);


        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_SCHEDULE_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_HALF_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_OP_HALF_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_SELF_TEST_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_OFFSET_MOVED_EVENT);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TOPIC_CONFIG_SYNC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_CONSUMER_OFFSET_SYNC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_DELAY_OFFSET_SYNC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_SUBSCRIPTION_GROUP_SYNC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_MESSAGE_MODE_SYNC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TIMER_METRICS_SYNC);


        // regex: ^[%|a-zA-Z0-9_-]+$
        // %
        VALID_CHAR_BIT_MAP['%'] = true;
        // -
        VALID_CHAR_BIT_MAP['-'] = true;
        // _
        VALID_CHAR_BIT_MAP['_'] = true;
        // |
        VALID_CHAR_BIT_MAP['|'] = true;
        for (int i = 0; i < VALID_CHAR_BIT_MAP.length; i++) {
            if (i >= '0' && i <= '9') {
                // 0-9
                VALID_CHAR_BIT_MAP[i] = true;
            } else if (i >= 'A' && i <= 'Z') {
                // A-Z
                VALID_CHAR_BIT_MAP[i] = true;
            } else if (i >= 'a' && i <= 'z') {
                // a-z
                VALID_CHAR_BIT_MAP[i] = true;
            }
        }
    }

    public static boolean isTopicOrGroupIllegal(String str) {
        int strLen = str.length();
        int len = VALID_CHAR_BIT_MAP.length;
        for (int i = 0; i < strLen; i++) {
            char ch = str.charAt(i);
            if (ch >= len || !VALID_CHAR_BIT_MAP[ch]) {
                return true;
            }
        }
        return false;
    }

    public static ValidateResult validateTopic(String topic) {

        if (UtilAll.isBlank(topic)) {
            return new ValidateResult(false, "The specified topic is blank.");
        }

        if (isTopicOrGroupIllegal(topic)) {
            String falseRemark = "The specified topic: " + topic + ", contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";
            return new ValidateResult(false, falseRemark);
        }

        if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
            if (topic.length() > RETRY_OR_DLQ_TOPIC_MAX_LENGTH) {
                String falseRemark = "The specified topic is DLQ or Retry topic: " + topic + ", and it's longer than topic max length: " + RETRY_OR_DLQ_TOPIC_MAX_LENGTH;
                return new ValidateResult(false, falseRemark);
            }
        } else {
            if (topic.length() > TOPIC_MAX_LENGTH) {
                String falseRemark = "The specified topic: " + topic + ", is longer than topic max length: " + TOPIC_MAX_LENGTH;
                return new ValidateResult(false, falseRemark);
            }
        }

        return new ValidateResult(true, "");
    }

    public static ValidateResult validateGroup(String group) {

        if (UtilAll.isBlank(group)) {
            return new ValidateResult(false, "The specified group is blank.");
        }

        if (isTopicOrGroupIllegal(group)) {
            String falseRemark = "The specified group: " + group + ", contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";
            return new ValidateResult(false, falseRemark);
        }

        if (group.length() > GROUP_MAX_LENGTH) {
            String falseRemark = "The specified group: " + group + ", is longer than group max length: " + GROUP_MAX_LENGTH;
            return new ValidateResult(false, falseRemark);
        }

        return new ValidateResult(true, "");
    }

    public static class ValidateResult {
        private final boolean valid;
        private final String remark;

        public ValidateResult(boolean valid, String remark) {
            this.valid = valid;
            this.remark = remark;
        }

        public boolean isValid() {
            return valid;
        }

        public String getRemark() {
            return remark;
        }
    }

    public static boolean isSystemTopic(String topic) {
        return SYSTEM_TOPIC_SET.contains(topic) || topic.startsWith(SYSTEM_TOPIC_PREFIX);
    }

    public static boolean isNotAllowedSendTopic(String topic) {
        return NOT_ALLOWED_SEND_TOPIC_SET.contains(topic);
    }

    public static void addSystemTopic(String systemTopic) {
        SYSTEM_TOPIC_SET.add(systemTopic);
    }

    public static Set<String> getSystemTopicSet() {
        return SYSTEM_TOPIC_SET;
    }

    public static Set<String> getNotAllowedSendTopicSet() {
        return NOT_ALLOWED_SEND_TOPIC_SET;
    }
}
