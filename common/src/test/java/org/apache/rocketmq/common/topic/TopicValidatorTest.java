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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicValidatorTest {

    @Test
    public void testTopicValidator_NotPass() {
        TopicValidator.ValidateResult res = TopicValidator.validateTopic("");
        assertThat(res.isValid()).isFalse();
        assertThat(res.getRemark()).contains("The specified topic is blank");

        res = TopicValidator.validateTopic("../TopicTest");
        assertThat(res.isValid()).isFalse();
        assertThat(res.getRemark()).contains("The specified topic contains illegal characters");

        res = TopicValidator.validateTopic(generateString(128));
        assertThat(res.isValid()).isFalse();
        assertThat(res.getRemark()).contains("The specified topic is longer than topic max length.");

        res = TopicValidator.validateTopic(generateString2(128));
        assertThat(res.isValid()).isFalse();
        assertThat(res.getRemark()).contains("The specified topic is longer than topic max length.");

        res = TopicValidator.validateTopic(generateRetryTopic(256));
        assertThat(res.isValid()).isFalse();
        assertThat(res.getRemark()).contains("The specified topic is longer than topic max length.");

        res = TopicValidator.validateTopic(generateDlqTopic(256));
        assertThat(res.isValid()).isFalse();
        assertThat(res.getRemark()).contains("The specified topic is longer than topic max length.");
    }

    @Test
    public void testTopicValidator_Pass() {
        TopicValidator.ValidateResult res = TopicValidator.validateTopic("TestTopic");
        assertThat(res.isValid()).isTrue();
        assertThat(res.getRemark()).isEmpty();

        res = TopicValidator.validateTopic(generateString2(127));
        assertThat(res.isValid()).isTrue();
        assertThat(res.getRemark()).isEmpty();

        res = TopicValidator.validateTopic(generateRetryTopic(255));
        assertThat(res.isValid()).isTrue();
        assertThat(res.getRemark()).isEmpty();

        res = TopicValidator.validateTopic(generateDlqTopic(255));
        assertThat(res.isValid()).isTrue();
        assertThat(res.getRemark()).isEmpty();
    }

    @Test
    public void testAddSystemTopic() {
        String topic = "SYSTEM_TOPIC_TEST";
        TopicValidator.addSystemTopic(topic);
        assertThat(TopicValidator.getSystemTopicSet()).contains(topic);
    }

    @Test
    public void testIsSystemTopic() {
        boolean res;
        for (String topic : TopicValidator.getSystemTopicSet()) {
            res = TopicValidator.isSystemTopic(topic);
            assertThat(res).isTrue();
        }

        String topic = TopicValidator.SYSTEM_TOPIC_PREFIX + "_test";
        res = TopicValidator.isSystemTopic(topic);
        assertThat(res).isTrue();

        topic = "test_not_system_topic";
        res = TopicValidator.isSystemTopic(topic);
        assertThat(res).isFalse();
    }

    @Test
    public void testIsSystemTopicWithResponse() {
        boolean res;
        for (String topic : TopicValidator.getSystemTopicSet()) {
            res = TopicValidator.isSystemTopic(topic);
            assertThat(res).isTrue();
        }

        String topic = "test_not_system_topic";
        res = TopicValidator.isSystemTopic(topic);
        assertThat(res).isFalse();
    }

    @Test
    public void testIsNotAllowedSendTopic() {
        boolean res;
        for (String topic : TopicValidator.getNotAllowedSendTopicSet()) {
            res = TopicValidator.isNotAllowedSendTopic(topic);
            assertThat(res).isTrue();
        }

        String topic = "test_allowed_send_topic";
        res = TopicValidator.isNotAllowedSendTopic(topic);
        assertThat(res).isFalse();
    }

    @Test
    public void testIsNotAllowedSendTopicWithResponse() {
        boolean res;
        for (String topic : TopicValidator.getNotAllowedSendTopicSet()) {
            res = TopicValidator.isNotAllowedSendTopic(topic);
            assertThat(res).isTrue();
        }

        String topic = "test_allowed_send_topic";
        res = TopicValidator.isNotAllowedSendTopic(topic);
        assertThat(res).isFalse();
    }

    private static String generateString(int length) {
        StringBuilder stringBuffer = new StringBuilder();
        String tmpStr = "0123456789";
        for (int i = 0; i < length; i++) {
            stringBuffer.append(tmpStr);
        }
        return stringBuffer.toString();
    }

    private static String generateString2(int length) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            stringBuilder.append("a");
        }
        return stringBuilder.toString();
    }

    private static String generateRetryTopic(int length) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("%RETRY%");
        for (int i = 0; i < length - 7; i++) {
            stringBuilder.append("a");
        }
        return stringBuilder.toString();
    }

    private static String generateDlqTopic(int length) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("%DLQ%");
        for (int i = 0; i < length - 5; i++) {
            stringBuilder.append("a");
        }
        return stringBuilder.toString();
    }
}
