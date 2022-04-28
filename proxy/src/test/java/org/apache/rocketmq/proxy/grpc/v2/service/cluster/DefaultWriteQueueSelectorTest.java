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
package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

public class DefaultWriteQueueSelectorTest extends BaseServiceTest {

    @Override
    public void beforeEach() throws Throwable {
        SelectableMessageQueue queue = new SelectableMessageQueue(
            new MessageQueue("topic", "selectOrderQueue", 0),
            "selectOrderQueueAddr");
        when(topicRouteCache.selectOneWriteQueueByKey(anyString(), anyString()))
            .thenReturn(queue);

        queue = new SelectableMessageQueue(
            new MessageQueue("topic", "selectNormalQueue", 0),
            "selectNormalQueueAddr");
        when(topicRouteCache.selectOneWriteQueue(anyString(), isNull()))
            .thenReturn(queue);
    }

    @Test
    public void selectWithShardingKey() {
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setResourceNamespace("namespace")
                    .setName("topic")
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("msgId")
                    .setMessageGroup("key")
                    .build())
                .setBody(ByteString.copyFrom("hello", StandardCharsets.UTF_8))
                .build())
            .build();
        WriteQueueSelector queueSelector = new DefaultWriteQueueSelector(this.topicRouteCache);
        SelectableMessageQueue queue = queueSelector.selectQueue(Context.current(), request);

        assertEquals("selectOrderQueue", queue.getBrokerName());
        assertEquals("selectOrderQueueAddr", queue.getBrokerAddr());
    }

    @Test
    public void selectNormalQueue() {
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setResourceNamespace("namespace")
                    .setName("topic")
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("msgId")
                    .build())
                .setBody(ByteString.copyFrom("hello", StandardCharsets.UTF_8))
                .build())
            .build();
        WriteQueueSelector queueSelector = new DefaultWriteQueueSelector(this.topicRouteCache);
        SelectableMessageQueue queue = queueSelector.selectQueue(Context.current(), request);

        assertEquals("selectNormalQueue", queue.getBrokerName());
        assertEquals("selectNormalQueueAddr", queue.getBrokerAddr());
    }
}