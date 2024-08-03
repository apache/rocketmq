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

package org.apache.rocketmq.broker.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerMetricsManagerTest {

    @Test
    public void testNewAttributesBuilder() {
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder().put("a", "b")
            .build();
        assertThat(attributes.get(AttributeKey.stringKey("a"))).isEqualTo("b");
    }

    @Test
    public void testCustomizedAttributesBuilder() {
        BrokerMetricsManager.attributesBuilderSupplier = () -> new AttributesBuilder() {
            private AttributesBuilder attributesBuilder = Attributes.builder();
            @Override
            public Attributes build() {
                return attributesBuilder.put("customized", "value").build();
            }

            @Override
            public <T> AttributesBuilder put(AttributeKey<Long> key, int value) {
                attributesBuilder.put(key, value);
                return this;
            }

            @Override
            public <T> AttributesBuilder put(AttributeKey<T> key, T value) {
                attributesBuilder.put(key, value);
                return this;
            }

            @Override
            public AttributesBuilder putAll(Attributes attributes) {
                attributesBuilder.putAll(attributes);
                return this;
            }
        };
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder().put("a", "b")
            .build();
        assertThat(attributes.get(AttributeKey.stringKey("a"))).isEqualTo("b");
        assertThat(attributes.get(AttributeKey.stringKey("customized"))).isEqualTo("value");
    }


    @Test
    public void testIsRetryOrDlqTopicWithRetryTopic() {
        String topic = MixAll.RETRY_GROUP_TOPIC_PREFIX + "TestTopic";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isTrue();
    }

    @Test
    public void testIsRetryOrDlqTopicWithDlqTopic() {
        String topic = MixAll.DLQ_GROUP_TOPIC_PREFIX + "TestTopic";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isTrue();
    }

    @Test
    public void testIsRetryOrDlqTopicWithNonRetryOrDlqTopic() {
        String topic = "NormalTopic";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsRetryOrDlqTopicWithEmptyTopic() {
        String topic = "";
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsRetryOrDlqTopicWithNullTopic() {
        String topic = null;
        boolean result = BrokerMetricsManager.isRetryOrDlqTopic(topic);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystemGroup_SystemGroup_ReturnsTrue() {
        String group = "FooGroup";
        String systemGroup = MixAll.CID_RMQ_SYS_PREFIX + group;
        boolean result = BrokerMetricsManager.isSystemGroup(systemGroup);
        assertThat(result).isTrue();
    }

    @Test
    public void testIsSystemGroup_NonSystemGroup_ReturnsFalse() {
        String group = "FooGroup";
        boolean result = BrokerMetricsManager.isSystemGroup(group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystemGroup_EmptyGroup_ReturnsFalse() {
        String group = "";
        boolean result = BrokerMetricsManager.isSystemGroup(group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystemGroup_NullGroup_ReturnsFalse() {
        String group = null;
        boolean result = BrokerMetricsManager.isSystemGroup(group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystem_SystemTopicOrSystemGroup_ReturnsTrue() {
        String topic = "FooTopic";
        String group = "FooGroup";
        String systemTopic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
        String systemGroup = MixAll.CID_RMQ_SYS_PREFIX + group;

        boolean resultTopic = BrokerMetricsManager.isSystem(systemTopic, group);
        assertThat(resultTopic).isTrue();

        boolean resultGroup = BrokerMetricsManager.isSystem(topic, systemGroup);
        assertThat(resultGroup).isTrue();
    }

    @Test
    public void testIsSystem_NonSystemTopicAndGroup_ReturnsFalse() {
        String topic = "FooTopic";
        String group = "FooGroup";
        boolean result = BrokerMetricsManager.isSystem(topic, group);
        assertThat(result).isFalse();
    }

    @Test
    public void testIsSystem_EmptyTopicAndGroup_ReturnsFalse() {
        String topic = "";
        String group = "";
        boolean result = BrokerMetricsManager.isSystem(topic, group);
        assertThat(result).isFalse();
    }

}