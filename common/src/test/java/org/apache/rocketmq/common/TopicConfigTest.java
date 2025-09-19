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

package org.apache.rocketmq.common;

import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicConfigTest {
    String topicName = "topic";
    int queueNums = 8;
    int perm = PermName.PERM_READ | PermName.PERM_WRITE;
    TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;

    @Test
    public void testEncode() {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setReadQueueNums(queueNums);
        topicConfig.setWriteQueueNums(queueNums);
        topicConfig.setPerm(perm);
        topicConfig.setTopicFilterType(topicFilterType);
        topicConfig.setTopicMessageType(TopicMessageType.FIFO);

        String encode = topicConfig.encode();
        assertThat(encode).isEqualTo("topic 8 8 6 SINGLE_TAG {\"+message.type\":\"FIFO\"}");
    }

    @Test
    public void testDecode() {
        String encode = "topic 8 8 6 SINGLE_TAG {\"+message.type\":\"FIFO\"}";
        TopicConfig decodeTopicConfig = new TopicConfig();
        decodeTopicConfig.decode(encode);

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setReadQueueNums(queueNums);
        topicConfig.setWriteQueueNums(queueNums);
        topicConfig.setPerm(perm);
        topicConfig.setTopicFilterType(topicFilterType);
        topicConfig.setTopicMessageType(TopicMessageType.FIFO);

        assertThat(decodeTopicConfig).isEqualTo(topicConfig);
    }

    @Test
    public void testDecodeWhenCompatible() {
        String encode = "topic 8 8 6 SINGLE_TAG";
        TopicConfig decodeTopicConfig = new TopicConfig();
        decodeTopicConfig.decode(encode);

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setReadQueueNums(queueNums);
        topicConfig.setWriteQueueNums(queueNums);
        topicConfig.setPerm(perm);
        topicConfig.setTopicFilterType(topicFilterType);

        assertThat(decodeTopicConfig).isEqualTo(topicConfig);
    }
}