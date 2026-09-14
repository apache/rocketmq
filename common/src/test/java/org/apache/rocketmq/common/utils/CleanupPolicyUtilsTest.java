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

package org.apache.rocketmq.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CleanupPolicyUtilsTest {

    @Test
    public void testGetDeletePolicyShouldReturnDefaultWhenTopicConfigAbsent() {
        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.empty());

        assertThat(policy).isEqualTo(CleanupPolicy.DELETE);
    }

    @Test
    public void testGetDeletePolicyShouldReturnDefaultWhenAttributesNull() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        topicConfig.setAttributes(null);

        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));

        assertThat(policy).isEqualTo(CleanupPolicy.DELETE);
    }

    @Test
    public void testGetDeletePolicyShouldReturnDefaultWhenAttributesEmpty() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        topicConfig.setAttributes(new HashMap<>());

        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));

        assertThat(policy).isEqualTo(CleanupPolicy.DELETE);
    }

    @Test
    public void testGetDeletePolicyShouldReturnDefaultWhenKeyMissing() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("some.other.key", "some-value");
        topicConfig.setAttributes(attributes);

        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));

        assertThat(policy).isEqualTo(CleanupPolicy.DELETE);
    }

    @Test
    public void testGetDeletePolicyShouldReturnDeleteWhenExplicitlySet() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getName(), "DELETE");
        topicConfig.setAttributes(attributes);

        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));

        assertThat(policy).isEqualTo(CleanupPolicy.DELETE);
    }

    @Test
    public void testGetDeletePolicyShouldReturnCompactionWhenSet() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getName(), "COMPACTION");
        topicConfig.setAttributes(attributes);

        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));

        assertThat(policy).isEqualTo(CleanupPolicy.COMPACTION);
    }

    @Test
    public void testIsCompactionShouldReturnTrueWhenPolicyIsCompaction() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getName(), "COMPACTION");
        topicConfig.setAttributes(attributes);

        boolean result = CleanupPolicyUtils.isCompaction(Optional.of(topicConfig));

        assertThat(result).isTrue();
    }

    @Test
    public void testIsCompactionShouldReturnFalseWhenPolicyIsDelete() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getName(), "DELETE");
        topicConfig.setAttributes(attributes);

        boolean result = CleanupPolicyUtils.isCompaction(Optional.of(topicConfig));

        assertThat(result).isFalse();
    }

    @Test
    public void testIsCompactionShouldReturnFalseWhenTopicConfigAbsent() {
        boolean result = CleanupPolicyUtils.isCompaction(Optional.empty());

        assertThat(result).isFalse();
    }

    @Test
    public void testIsCompactionShouldReturnFalseWhenAttributesNull() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        topicConfig.setAttributes(null);

        boolean result = CleanupPolicyUtils.isCompaction(Optional.of(topicConfig));

        assertThat(result).isFalse();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDeletePolicyShouldThrowWhenAttributeValueInvalid() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.CLEANUP_POLICY_ATTRIBUTE.getName(), "INVALID_VALUE");
        topicConfig.setAttributes(attributes);

        CleanupPolicyUtils.getDeletePolicy(Optional.of(topicConfig));
    }
}
