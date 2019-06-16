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

import io.openmessaging.KeyValue;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.internal.DefaultKeyValue;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
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
    @Mock
    private ConsumeRequest consumeRequest1;

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

    @Test
    public void testBatchPollMessage() throws Exception {
        byte[] body = new byte[] {'1', '2', '3'};
        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(body);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic("HELLO_QUEUE");

        byte[] body1 = new byte[] {'4', '5', '6'};
        MessageExt consumedMsg1 = new MessageExt();
        consumedMsg1.setMsgId("NewMsgId1");
        consumedMsg1.setBody(body1);
        consumedMsg1.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg1.setTopic("HELLO_QUEUE1");

        when(consumeRequest.getMessageExt()).thenReturn(consumedMsg);
        when(consumeRequest1.getMessageExt()).thenReturn(consumedMsg1);
        localMessageCache.submitConsumeRequest(consumeRequest);
        localMessageCache.submitConsumeRequest(consumeRequest1);
        KeyValue properties = new DefaultKeyValue();
        properties.put(NonStandardKeys.TIMEOUT, 3000);
        List<MessageExt> messageExts = localMessageCache.batchPoll(properties);
        assertThat(messageExts.size()).isEqualTo(2);
        MessageExt messageExt1 = null;
        MessageExt messageExt2 = null;
        for (MessageExt messageExt : messageExts) {
            if (messageExt.getMsgId().equals("NewMsgId")) {
                messageExt1 = messageExt;
            }
            if (messageExt.getMsgId().equals("NewMsgId1")) {
                messageExt2 = messageExt;
            }
        }
        assertThat(messageExt1).isNotNull();
        assertThat(messageExt2).isNotNull();
    }

    @Test
    public void getQueueMetaData() throws MQClientException {
        MessageQueue messageQueue1 = new MessageQueue("topic1", "brockerName1", 0);
        MessageQueue messageQueue2 = new MessageQueue("topic1", "brockerName2", 1);
        MessageQueue messageQueue3 = new MessageQueue("topic1", "brockerName3", 2);
        Set<MessageQueue> messageQueues = new HashSet<MessageQueue>() {
            {
                add(messageQueue1);
                add(messageQueue2);
                add(messageQueue3);
            }
        };

        when(rocketmqPullConsume.fetchSubscribeMessageQueues("topic1")).thenReturn(messageQueues);
        Set<QueueMetaData> queueMetaDatas = localMessageCache.getQueueMetaData("topic1");
        assertThat(queueMetaDatas.size()).isEqualTo(3);
        QueueMetaData queueMetaData1 = null;
        QueueMetaData queueMetaData2 = null;
        QueueMetaData queueMetaData3 = null;
        for (QueueMetaData queueMetaData : queueMetaDatas) {
            if (queueMetaData.partitionId() == 0) {
                queueMetaData1 = queueMetaData;
            }
            if (queueMetaData.partitionId() == 1) {
                queueMetaData2 = queueMetaData;
            }
            if (queueMetaData.partitionId() == 2) {
                queueMetaData3 = queueMetaData;
            }
        }
        assertThat(queueMetaData1).isNotNull();
        assertThat(queueMetaData2).isNotNull();
        assertThat(queueMetaData3).isNotNull();
    }
}