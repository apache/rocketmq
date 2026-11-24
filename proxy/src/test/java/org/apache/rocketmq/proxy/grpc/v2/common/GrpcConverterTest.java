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

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GrpcConverterTest {
    @Test
    public void testBuildMessageQueue() {
        String topic = "topic";
        String brokerName = "brokerName";
        int queueId = 1;
        MessageExt messageExt = new MessageExt();
        messageExt.setQueueId(queueId);
        messageExt.setTopic(topic);

        MessageQueue messageQueue = GrpcConverter.getInstance().buildMessageQueue(messageExt, brokerName);
        assertThat(messageQueue.getTopic().getName()).isEqualTo(topic);
        assertThat(messageQueue.getBroker().getName()).isEqualTo(brokerName);
        assertThat(messageQueue.getId()).isEqualTo(queueId);
    }

    @Test
    public void testBuildMessageWithLiteTopic() {
        final String topic = "test-topic";
        final String liteTopic = "test-lite-topic";
        // Build a message with lite topic properties
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setBody("test-body".getBytes(StandardCharsets.UTF_8));
        messageExt.setQueueId(1);
        messageExt.setQueueOffset(100L);
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setStoreTimestamp(System.currentTimeMillis());
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 1234));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 5678));
        messageExt.setReconsumeTimes(0);
        messageExt.setMsgId("test-msg-id");

        // Set lite topic property
        MessageAccessor.setLiteTopic(messageExt, liteTopic);

        // Convert message
        GrpcConverter grpcConverter = GrpcConverter.getInstance();
        apache.rocketmq.v2.Message grpcMessage = grpcConverter.buildMessage(messageExt);

        // Verify basic properties
        assertNotNull(grpcMessage);
        assertEquals(topic, grpcMessage.getTopic().getName());
        assertEquals("test-body", grpcMessage.getBody().toString(StandardCharsets.UTF_8));

        // Verify lite topic in system properties
        assertNotNull(grpcMessage.getSystemProperties());
        assertTrue(grpcMessage.getSystemProperties().hasLiteTopic());
        assertEquals(liteTopic, grpcMessage.getSystemProperties().getLiteTopic());

        // Verify message type is LITE
        assertEquals(MessageType.LITE, grpcMessage.getSystemProperties().getMessageType());
    }
}