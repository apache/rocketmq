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

package org.apache.rocketmq.proxy.processor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProducerProcessorTest extends BaseProcessorTest {

    private static final String PRODUCER_GROUP = "producerGroup";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String TOPIC = "topic";

    private ProducerProcessor producerProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        this.producerProcessor = new ProducerProcessor(this.messagingProcessor, this.serviceManager, Executors.newCachedThreadPool());
    }

    @Test
    public void testSendMessage() throws Throwable {
        when(metadataService.getTopicMessageType(any(), eq(TOPIC))).thenReturn(TopicMessageType.NORMAL);
        String txId = MessageClientIDSetter.createUniqID();
        String msgId = MessageClientIDSetter.createUniqID();
        long commitLogOffset = 1000L;
        long queueOffset = 100L;

        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setTransactionId(txId);
        sendResult.setMsgId(msgId);
        sendResult.setOffsetMsgId(createOffsetMsgId(commitLogOffset));
        sendResult.setQueueOffset(queueOffset);
        ArgumentCaptor<SendMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(SendMessageRequestHeader.class);
        when(this.messageService.sendMessage(any(), any(), any(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(sendResult)));

        List<Message> messageList = new ArrayList<>();
        Message messageExt = createMessageExt(TOPIC, "tag", 0, 0);
        messageList.add(messageExt);
        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        when(messageQueue.getBrokerName()).thenReturn("mockBroker");

        ArgumentCaptor<String> brokerNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> tranStateTableOffsetCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> commitLogOffsetCaptor = ArgumentCaptor.forClass(Long.class);
        when(transactionService.addTransactionDataByBrokerName(
            any(),
            brokerNameCaptor.capture(),
            anyString(),
            anyString(),
            tranStateTableOffsetCaptor.capture(),
            commitLogOffsetCaptor.capture(),
            anyString(), any())).thenReturn(mock(TransactionData.class));

        List<SendResult> sendResultList = this.producerProcessor.sendMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            PRODUCER_GROUP,
            MessageSysFlag.TRANSACTION_PREPARED_TYPE,
            messageList,
            3000
        ).get();

        assertNotNull(sendResultList);
        assertEquals("mockBroker", brokerNameCaptor.getValue());
        assertEquals(queueOffset, tranStateTableOffsetCaptor.getValue().longValue());
        assertEquals(commitLogOffset, commitLogOffsetCaptor.getValue().longValue());

        SendMessageRequestHeader requestHeader = requestHeaderArgumentCaptor.getValue();
        assertEquals(PRODUCER_GROUP, requestHeader.getProducerGroup());
        assertEquals(TOPIC, requestHeader.getTopic());
    }

    @Test
    public void testSendRetryMessage() throws Throwable {
        String txId = MessageClientIDSetter.createUniqID();
        String msgId = MessageClientIDSetter.createUniqID();
        long commitLogOffset = 1000L;
        long queueOffset = 100L;

        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setTransactionId(txId);
        sendResult.setMsgId(msgId);
        sendResult.setOffsetMsgId(createOffsetMsgId(commitLogOffset));
        sendResult.setQueueOffset(queueOffset);
        ArgumentCaptor<SendMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(SendMessageRequestHeader.class);
        when(this.messageService.sendMessage(any(), any(), any(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(sendResult)));

        List<Message> messageExtList = new ArrayList<>();
        Message messageExt = createMessageExt(MixAll.getRetryTopic(CONSUMER_GROUP), "tag", 0, 0);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_RECONSUME_TIME, "1");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_MAX_RECONSUME_TIMES, "16");
        messageExtList.add(messageExt);
        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        when(messageQueue.getBrokerName()).thenReturn("mockBroker");

        ArgumentCaptor<String> brokerNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> tranStateTableOffsetCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> commitLogOffsetCaptor = ArgumentCaptor.forClass(Long.class);
        when(transactionService.addTransactionDataByBrokerName(
            any(),
            brokerNameCaptor.capture(),
            anyString(),
            anyString(),
            tranStateTableOffsetCaptor.capture(),
            commitLogOffsetCaptor.capture(),
            anyString(), any())).thenReturn(mock(TransactionData.class));

        List<SendResult> sendResultList = this.producerProcessor.sendMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            PRODUCER_GROUP,
            MessageSysFlag.TRANSACTION_PREPARED_TYPE,
            messageExtList,
            3000
        ).get();

        assertNotNull(sendResultList);
        assertEquals("mockBroker", brokerNameCaptor.getValue());
        assertEquals(queueOffset, tranStateTableOffsetCaptor.getValue().longValue());
        assertEquals(commitLogOffset, commitLogOffsetCaptor.getValue().longValue());

        SendMessageRequestHeader requestHeader = requestHeaderArgumentCaptor.getValue();
        assertEquals(PRODUCER_GROUP, requestHeader.getProducerGroup());
        assertEquals(MixAll.getRetryTopic(CONSUMER_GROUP), requestHeader.getTopic());
        assertEquals(1, requestHeader.getReconsumeTimes().intValue());
        assertEquals(16, requestHeader.getMaxReconsumeTimes().intValue());
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() throws Throwable {
        ArgumentCaptor<ConsumerSendMsgBackRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(ConsumerSendMsgBackRequestHeader.class);
        when(this.messageService.sendMessageBack(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(RemotingCommand.class)));

        MessageExt messageExt = createMessageExt(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), "", 16, 3000);
        RemotingCommand remotingCommand = this.producerProcessor.forwardMessageToDeadLetterQueue(
            createContext(),
            create(messageExt),
            messageExt.getMsgId(),
            CONSUMER_GROUP,
            TOPIC,
            3000
        ).get();

        assertNotNull(remotingCommand);
        ConsumerSendMsgBackRequestHeader requestHeader = requestHeaderArgumentCaptor.getValue();
        assertEquals(messageExt.getTopic(), requestHeader.getOriginTopic());
        assertEquals(messageExt.getMsgId(), requestHeader.getOriginMsgId());
        assertEquals(CONSUMER_GROUP, requestHeader.getGroup());
    }

    private static String createOffsetMsgId(long commitLogOffset) {
        int msgIDLength = 4 + 4 + 8;
        ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
        return MessageDecoder.createMessageId(byteBufferMsgId,
            MessageExt.socketAddress2ByteBuffer(NetworkUtil.string2SocketAddress("127.0.0.1:10911")),
            commitLogOffset);
    }
}
