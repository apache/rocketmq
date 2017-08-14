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
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LocalMessageCacheTest {
    private LocalMessageCache localMessageCache;
    @Mock
    private DefaultMQPullConsumer rocketmqPullConsume;
    @Mock
    private ConsumeRequest consumeRequest;

    @Before
    public void init() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRmqPullMessageBatchNums(512);
        clientConfig.setRmqPullMessageCacheCapacity(1024);
        localMessageCache = new LocalMessageCache(rocketmqPullConsume, clientConfig);
    }

    @Test
    public void testNextPullBatchNums() throws Exception {
        assertThat(localMessageCache.nextPullBatchNums()).isEqualTo(512);
        for (int i = 0; i < 513; i++) {
            localMessageCache.submitConsumeRequest(consumeRequest);
        }
        assertThat(localMessageCache.nextPullBatchNums()).isEqualTo(511);
    }

    @Test
    public void testNextPullOffset() throws Exception {
        MessageQueue messageQueue = new MessageQueue();
        when(rocketmqPullConsume.fetchConsumeOffset(any(MessageQueue.class), anyBoolean()))
            .thenReturn(123L);
        assertThat(localMessageCache.nextPullOffset(new MessageQueue())).isEqualTo(123L);
    }

    @Test
    public void testUpdatePullOffset() throws Exception {
        MessageQueue messageQueue = new MessageQueue();
        localMessageCache.updatePullOffset(messageQueue, 124L);
        assertThat(localMessageCache.nextPullOffset(messageQueue)).isEqualTo(124L);
    }

    @Test
    public void testSubmitConsumeRequest() throws Exception {
        byte[] body = new byte[] {'1', '2', '3'};
        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(body);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic("HELLO_QUEUE");

        when(consumeRequest.getMessageExt()).thenReturn(consumedMsg);
        localMessageCache.submitConsumeRequest(consumeRequest);
        assertThat(localMessageCache.poll()).isEqualTo(consumedMsg);
    }
}