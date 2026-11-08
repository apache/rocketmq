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

import java.util.Arrays;
import java.util.Set;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageUtilsTest {

    private static final String NAME_VALUE_SEPARATOR = "\u0001";
    private static final String PROPERTY_SEPARATOR = "\u0002";

    @Test
    public void testGetShardingKeyIndexIsDeterministicAndInRange() {
        for (int i = 0; i < 100; i++) {
            String shardingKey = "sharding-key-" + i;
            int index = MessageUtils.getShardingKeyIndex(shardingKey, 8);

            assertThat(index).isBetween(0, 7);
            assertThat(MessageUtils.getShardingKeyIndex(shardingKey, 8)).isEqualTo(index);
        }
    }

    @Test
    public void testGetShardingKeyIndexWithEmptyKey() {
        assertThat(MessageUtils.getShardingKeyIndex("", 4)).isBetween(0, 3);
    }

    @Test
    public void testGetShardingKeyIndexByMsgUsesShardingKeyProperty() {
        MessageExt message = new MessageExt();
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_SHARDING_KEY, "order-123");

        assertThat(MessageUtils.getShardingKeyIndexByMsg(message, 8))
            .isEqualTo(MessageUtils.getShardingKeyIndex("order-123", 8));
    }

    @Test
    public void testGetShardingKeyIndexByMsgFallsBackToEmptyKey() {
        MessageExt message = new MessageExt();

        assertThat(MessageUtils.getShardingKeyIndexByMsg(message, 8))
            .isEqualTo(MessageUtils.getShardingKeyIndex("", 8));
    }

    @Test
    public void testGetShardingKeyIndexesDeduplicatesSameKey() {
        MessageExt first = new MessageExt();
        MessageAccessor.putProperty(first, MessageConst.PROPERTY_SHARDING_KEY, "same-key");
        MessageExt second = new MessageExt();
        MessageAccessor.putProperty(second, MessageConst.PROPERTY_SHARDING_KEY, "same-key");

        Set<Integer> indexes = MessageUtils.getShardingKeyIndexes(Arrays.asList(first, second), 8);

        assertThat(indexes).hasSize(1);
        assertThat(indexes).contains(MessageUtils.getShardingKeyIndex("same-key", 8));
    }

    @Test
    public void testDeletePropertyWithNullProperties() {
        assertThat(MessageUtils.deleteProperty(null, "KeyA")).isNull();
    }

    @Test
    public void testDeletePropertyKeepsUnrelatedProperties() {
        String properties = property("KeyA", "ValueA") + PROPERTY_SEPARATOR + property("KeyB", "ValueB");

        assertThat(MessageUtils.deleteProperty(properties, "KeyC")).isEqualTo(properties);
    }

    @Test
    public void testDeletePropertyAtTheBeginning() {
        String properties = property("KeyA", "ValueA") + PROPERTY_SEPARATOR + property("KeyB", "ValueB");

        assertThat(MessageUtils.deleteProperty(properties, "KeyA"))
            .isEqualTo(property("KeyB", "ValueB"));
    }

    @Test
    public void testDeletePropertyInTheMiddle() {
        String properties = property("KeyA", "ValueA") + PROPERTY_SEPARATOR
            + property("KeyB", "ValueB") + PROPERTY_SEPARATOR
            + property("KeyC", "ValueC");

        assertThat(MessageUtils.deleteProperty(properties, "KeyB"))
            .isEqualTo(property("KeyA", "ValueA") + PROPERTY_SEPARATOR + property("KeyC", "ValueC"));
    }

    @Test
    public void testDeleteLastPropertyKeepsTrailingSeparator() {
        String properties = property("KeyA", "ValueA") + PROPERTY_SEPARATOR + property("KeyB", "ValueB");

        // The trailing separator is preserved by design, consistent with the broker's
        // property layout where each entry is followed by a separator.
        assertThat(MessageUtils.deleteProperty(properties, "KeyB"))
            .isEqualTo(property("KeyA", "ValueA") + PROPERTY_SEPARATOR);
    }

    @Test
    public void testDeleteOnlyProperty() {
        assertThat(MessageUtils.deleteProperty(property("KeyA", "ValueA"), "KeyA")).isEmpty();
    }

    @Test
    public void testDeleteAllOccurrencesOfSameProperty() {
        String properties = property("KeyA", "ValueA") + PROPERTY_SEPARATOR + property("KeyA", "ValueA2");

        assertThat(MessageUtils.deleteProperty(properties, "KeyA")).isEmpty();
    }

    @Test
    public void testDeletePropertyDoesNotMatchNamePrefix() {
        String properties = property("KeyA", "ValueA") + PROPERTY_SEPARATOR + property("KeyAB", "ValueB");

        assertThat(MessageUtils.deleteProperty(properties, "KeyA"))
            .isEqualTo(property("KeyAB", "ValueB"));
    }

    @Test
    public void testDeletePropertyRequiresNameValueSeparator() {
        String properties = "KeyA" + PROPERTY_SEPARATOR + property("KeyB", "ValueB");

        assertThat(MessageUtils.deleteProperty(properties, "KeyA")).isEqualTo(properties);
    }

    private static String property(String name, String value) {
        return name + NAME_VALUE_SEPARATOR + value;
    }
}
