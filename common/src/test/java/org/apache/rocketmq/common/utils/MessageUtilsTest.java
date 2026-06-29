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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageConst;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageUtilsTest {

    @Test
    public void testGetShardingKeyIndex() {
        String shardingKey = "test-key";
        int indexSize = 10;
        int index = MessageUtils.getShardingKeyIndex(shardingKey, indexSize);
        assertThat(index).isBetween(0, indexSize - 1);
    }

    @Test
    public void testGetShardingKeyIndex_EmptyKey() {
        String shardingKey = "";
        int indexSize = 5;
        int index = MessageUtils.getShardingKeyIndex(shardingKey, indexSize);
        assertThat(index).isBetween(0, indexSize - 1);
    }

    @Test
    public void testGetShardingKeyIndex_ChineseKey() {
        String shardingKey = "测试分片键";
        int indexSize = 100;
        int index = MessageUtils.getShardingKeyIndex(shardingKey, indexSize);
        assertThat(index).isBetween(0, indexSize - 1);
    }

    @Test
    public void testGetShardingKeyIndex_DifferentKeys() {
        int indexSize = 10;
        int index1 = MessageUtils.getShardingKeyIndex("user-1", indexSize);
        int index2 = MessageUtils.getShardingKeyIndex("user-2", indexSize);
        assertThat(index1).isBetween(0, indexSize - 1);
        assertThat(index2).isBetween(0, indexSize - 1);
    }

    @Test
    public void testGetShardingKeyIndexByMsg() {
        MessageExt msg = new MessageExt();
        msg.putUserProperty(MessageConst.PROPERTY_SHARDING_KEY, "sharding-123");
        int index = MessageUtils.getShardingKeyIndexByMsg(msg, 10);
        assertThat(index).isBetween(0, 9);
    }

    @Test
    public void testGetShardingKeyIndexByMsg_NullShardingKey() {
        MessageExt msg = new MessageExt();
        int index = MessageUtils.getShardingKeyIndexByMsg(msg, 10);
        assertThat(index).isBetween(0, 9);
    }

    @Test
    public void testGetShardingKeyIndexes() {
        MessageExt msg1 = new MessageExt();
        msg1.putUserProperty(MessageConst.PROPERTY_SHARDING_KEY, "key-1");
        MessageExt msg2 = new MessageExt();
        msg2.putUserProperty(MessageConst.PROPERTY_SHARDING_KEY, "key-2");
        MessageExt msg3 = new MessageExt();
        msg3.putUserProperty(MessageConst.PROPERTY_SHARDING_KEY, "key-1");
        Set<Integer> indexes = MessageUtils.getShardingKeyIndexes(Arrays.asList(msg1, msg2, msg3), 10);
        assertThat(indexes).isNotEmpty();
        assertThat(indexes).allMatch(i -> i >= 0 && i < 10);
    }

    @Test
    public void testGetShardingKeyIndexes_EmptyCollection() {
        Set<Integer> indexes = MessageUtils.getShardingKeyIndexes(Arrays.asList(), 10);
        assertThat(indexes).isEmpty();
    }

    @Test
    public void testDeleteProperty_FromSimpleString() {
        String properties = "key1=value1;key2=value2;key3=value3";
        String result = MessageUtils.deleteProperty(properties, "key2");
        assertThat(result).isEqualTo("key1=value1;key3=value3");
        assertThat(result).doesNotContain("key2");
    }

    @Test
    public void testDeleteProperty_FromNullString() {
        String result = MessageUtils.deleteProperty(null, "key1");
        assertThat(result).isNull();
    }

    @Test
    public void testDeleteProperty_KeyNotFound() {
        String properties = "key1=value1;key2=value2";
        String result = MessageUtils.deleteProperty(properties, "key3");
        assertThat(result).isEqualTo("key1=value1;key2=value2");
    }

    @Test
    public void testDeleteProperty_DeleteFirstKey() {
        String properties = "first=value1;second=value2";
        String result = MessageUtils.deleteProperty(properties, "first");
        assertThat(result).isEqualTo("second=value2");
        assertThat(result).doesNotContain("first");
    }

    @Test
    public void testDeleteProperty_DeleteLastKey() {
        String properties = "first=value1;last=value2";
        String result = MessageUtils.deleteProperty(properties, "last");
        assertThat(result).isEqualTo("first=value1");
        assertThat(result).doesNotContain("last");
    }

    @Test
    public void testDeleteProperty_MultipleSameKeys() {
        String properties = "key=value1;other=value2;key=value3";
        String result = MessageUtils.deleteProperty(properties, "key");
        assertThat(result).isEqualTo("other=value2");
        assertThat(result).doesNotContain("key=");
    }

    @Test
    public void testDeleteProperty_SubstringKeyMatch() {
        String properties = "mykey=value1;key=value2;other=value3";
        String result = MessageUtils.deleteProperty(properties, "key");
        assertThat(result).contains("mykey=value1");
        assertThat(result).doesNotContain("key=value2");
    }

    @Test
    public void testDeleteProperty_ValueContainsKeyName() {
        String properties = "topic=my-topic;msgId=abc123";
        String result = MessageUtils.deleteProperty(properties, "msgId");
        assertThat(result).isEqualTo("topic=my-topic");
        assertThat(result).doesNotContain("msgId");
    }
}
