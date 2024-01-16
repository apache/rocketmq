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
}