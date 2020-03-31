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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;

public class ValidatorsTest {

    @Test
    public void testCheckName_Success() throws MQClientException {
        Validators.checkName("Hello");
        Validators.checkName("%RETRY%Hello");
        Validators.checkName("_%RETRY%Hello");
        Validators.checkName("-%RETRY%Hello");
        Validators.checkName("223-%RETRY%Hello");
    }

    @Test
    public void testCheckName_HasIllegalCharacters() {
        String illegalName = "Name&*^";
        try {
            Validators.checkName(illegalName);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith(String.format("The specified [%s] contains illegal characters, allowing only %s", illegalName, Validators.VALID_PATTERN_STR));
        }
    }

    @Test
    public void testCheckTopic_UseDefaultTopic() {
        String defaultTopic = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;
        try {
            Validators.checkName(defaultTopic);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith(String.format("The [%s] is conflict with default.", defaultTopic));
        }
    }

    @Test
    public void testCheckName_BlankName() {
        String blankName = "";
        try {
            Validators.checkName(blankName);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith("The specified name is blank");
        }
    }

    @Test
    public void testCheckName_TooLongTopic() {
        String tooLongName = StringUtils.rightPad("TooLongName", Validators.CHARACTER_MAX_LENGTH + 1, "_");
        assertThat(tooLongName.length()).isGreaterThan(Validators.CHARACTER_MAX_LENGTH);
    }

    public void testCheckTopic_TooLongTopic(String tooLongName) {
        String tooLongTopic = StringUtils.rightPad("TooLongTopic", Validators.TOPIC_MAX_LENGTH + 1, "_");
        assertThat(tooLongTopic.length()).isGreaterThan(Validators.TOPIC_MAX_LENGTH);
        try {
            Validators.checkName(tooLongName);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith(String.format("The specified %s is longer than topic max length %s.",tooLongName,Validators.CHARACTER_MAX_LENGTH));
            assertThat(e).hasMessageStartingWith("The specified topic is longer than topic max length");
        }
    }
}
