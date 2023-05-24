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

package org.apache.rocketmq.client;

import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.fail;

public class ValidatorsTest {

    @Test
    public void testGroupNameBlank() {
        try {
            Validators.checkGroup(null);
            fail("excepted MQClientException for group name is blank");
        } catch (MQClientException e) {
            assertThat(e.getErrorMessage()).isEqualTo("the specified group is blank");
        }
    }

    @Test
    public void testCheckTopic_Success() throws MQClientException {
        Validators.checkTopic("Hello");
        Validators.checkTopic("%RETRY%Hello");
        Validators.checkTopic("_%RETRY%Hello");
        Validators.checkTopic("-%RETRY%Hello");
        Validators.checkTopic("223-%RETRY%Hello");
    }

    @Test
    public void testCheckTopic_HasIllegalCharacters() {
        String illegalTopic = "TOPIC&*^";
        try {
            Validators.checkTopic(illegalTopic);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith(String.format("The specified topic[%s] contains illegal characters, allowing only %s", illegalTopic, "^[%|a-zA-Z0-9_-]+$"));
        }
    }

    @Test
    public void testCheckTopic_BlankTopic() {
        String blankTopic = "";
        try {
            Validators.checkTopic(blankTopic);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith("The specified topic is blank");
        }
    }

    @Test
    public void testCheckTopic_TooLongTopic() {
        String tooLongTopic = StringUtils.rightPad("TooLongTopic", Validators.TOPIC_MAX_LENGTH + 1, "_");
        assertThat(tooLongTopic.length()).isGreaterThan(Validators.TOPIC_MAX_LENGTH);
        try {
            Validators.checkTopic(tooLongTopic);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith("The specified topic is longer than topic max length");
        }
    }

    @Test
    public void testIsSystemTopic() {
        for (String topic : TopicValidator.getSystemTopicSet()) {
            try {
                Validators.isSystemTopic(topic);
                fail("excepted MQClientException for system topic");
            } catch (MQClientException e) {
                assertThat(e.getResponseCode()).isEqualTo(-1);
                assertThat(e.getErrorMessage()).isEqualTo(String.format("The topic[%s] is conflict with system topic.", topic));
            }
        }
    }

    @Test
    public void testIsNotAllowedSendTopic() {
        for (String topic : TopicValidator.getNotAllowedSendTopicSet()) {
            try {
                Validators.isNotAllowedSendTopic(topic);
                fail("excepted MQClientException for blacklist topic");
            } catch (MQClientException e) {
                assertThat(e.getResponseCode()).isEqualTo(-1);
                assertThat(e.getErrorMessage()).isEqualTo(String.format("Sending message to topic[%s] is forbidden.", topic));
            }
        }
    }

    @Test
    public void testTopicConfigValid() throws MQClientException {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setPerm(PermName.PERM_INHERIT | PermName.PERM_WRITE | PermName.PERM_READ);
        Validators.checkTopicConfig(topicConfig);

        topicConfig.setPerm(PermName.PERM_WRITE | PermName.PERM_READ);
        Validators.checkTopicConfig(topicConfig);

        topicConfig.setPerm(PermName.PERM_READ);
        Validators.checkTopicConfig(topicConfig);

        try {
            topicConfig.setPerm(PermName.PERM_PRIORITY);
            Validators.checkTopicConfig(topicConfig);
        } catch (MQClientException e) {
            assertThat(e.getResponseCode()).isEqualTo(ResponseCode.NO_PERMISSION);
            assertThat(e.getErrorMessage()).isEqualTo(String.format("topicPermission value: %s is invalid.", topicConfig.getPerm()));
        }

        try {
            topicConfig.setPerm(PermName.PERM_PRIORITY | PermName.PERM_WRITE);
            Validators.checkTopicConfig(topicConfig);
        } catch (MQClientException e) {
            assertThat(e.getResponseCode()).isEqualTo(ResponseCode.NO_PERMISSION);
            assertThat(e.getErrorMessage()).isEqualTo(String.format("topicPermission value: %s is invalid.", topicConfig.getPerm()));
        }
    }

    @Test
    public void testBrokerConfigValid() throws MQClientException {
        Properties brokerConfig = new Properties();
        brokerConfig.setProperty("brokerPermission",
            String.valueOf(PermName.PERM_INHERIT | PermName.PERM_WRITE | PermName.PERM_READ));
        Validators.checkBrokerConfig(brokerConfig);

        brokerConfig.setProperty("brokerPermission", String.valueOf(PermName.PERM_WRITE | PermName.PERM_READ));
        Validators.checkBrokerConfig(brokerConfig);

        brokerConfig.setProperty("brokerPermission", String.valueOf(PermName.PERM_READ));
        Validators.checkBrokerConfig(brokerConfig);

        try {
            brokerConfig.setProperty("brokerPermission", String.valueOf(PermName.PERM_PRIORITY));
            Validators.checkBrokerConfig(brokerConfig);
        } catch (MQClientException e) {
            assertThat(e.getResponseCode()).isEqualTo(ResponseCode.NO_PERMISSION);
            assertThat(e.getErrorMessage()).isEqualTo(String.format("brokerPermission value: %s is invalid.", brokerConfig.getProperty("brokerPermission")));
        }

        try {
            brokerConfig.setProperty("brokerPermission", String.valueOf(PermName.PERM_PRIORITY | PermName.PERM_INHERIT));
            Validators.checkBrokerConfig(brokerConfig);
        } catch (MQClientException e) {
            assertThat(e.getResponseCode()).isEqualTo(ResponseCode.NO_PERMISSION);
            assertThat(e.getErrorMessage()).isEqualTo(String.format("brokerPermission value: %s is invalid.", brokerConfig.getProperty("brokerPermission")));
        }
    }
}
