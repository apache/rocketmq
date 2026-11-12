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
import org.apache.rocketmq.common.attribute.CQType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class QueueTypeUtilsTest {

    @Test
    public void testGetCQTypeWithAbsentTopicConfig() {
        assertThat(QueueTypeUtils.getCQType(Optional.empty())).isEqualTo(CQType.SimpleCQ);
        assertThat(QueueTypeUtils.isBatchCq(Optional.empty())).isFalse();
    }

    @Test
    public void testGetCQTypeWithNullAttributes() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        topicConfig.setAttributes(null);

        assertThat(QueueTypeUtils.getCQType(Optional.of(topicConfig))).isEqualTo(CQType.SimpleCQ);
    }

    @Test
    public void testGetCQTypeWithEmptyAttributes() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        topicConfig.setAttributes(new HashMap<>());

        assertThat(QueueTypeUtils.getCQType(Optional.of(topicConfig))).isEqualTo(CQType.SimpleCQ);
    }

    @Test
    public void testGetCQTypeWithUnrelatedAttributes() {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("cleanup.policy", "DELETE");
        topicConfig.setAttributes(attributes);

        assertThat(QueueTypeUtils.getCQType(Optional.of(topicConfig))).isEqualTo(CQType.SimpleCQ);
    }

    @Test
    public void testGetCQTypeWithExplicitSimpleCQ() {
        TopicConfig topicConfig = topicConfigWithQueueType("SimpleCQ");

        assertThat(QueueTypeUtils.getCQType(Optional.of(topicConfig))).isEqualTo(CQType.SimpleCQ);
        assertThat(QueueTypeUtils.isBatchCq(Optional.of(topicConfig))).isFalse();
    }

    @Test
    public void testGetCQTypeWithExplicitBatchCQ() {
        TopicConfig topicConfig = topicConfigWithQueueType("BatchCQ");

        assertThat(QueueTypeUtils.getCQType(Optional.of(topicConfig))).isEqualTo(CQType.BatchCQ);
        assertThat(QueueTypeUtils.isBatchCq(Optional.of(topicConfig))).isTrue();
    }

    private static TopicConfig topicConfigWithQueueType(String queueType) {
        TopicConfig topicConfig = new TopicConfig("test-topic");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName(), queueType);
        topicConfig.setAttributes(attributes);
        return topicConfig;
    }
}
