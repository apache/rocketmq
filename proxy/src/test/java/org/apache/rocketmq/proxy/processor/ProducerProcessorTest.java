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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
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
        when(metadataService.getTopicMessageType(eq(TOPIC))).thenReturn(TopicMessageType.NORMAL);
        String txId = MessageClientIDSetter.createUniqID();
        String msgId = MessageClientIDSetter.createUniqID();

        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setTransactionId(txId);
        sendResult.setMsgId(msgId);
        ArgumentCaptor<SendMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(SendMessageRequestHeader.class);
        when(this.messageService.sendMessage(any(), any(), any(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(sendResult)));

        List<MessageExt> messageExtList = new ArrayList<>();
        MessageExt messageExt = createMessageExt(TOPIC, "tag", 0, 0);
        messageExt.setSysFlag(MessageSysFlag.TRANSACTION_PREPARED_TYPE);
        messageExtList.add(messageExt);
        SelectableMessageQueue messageQueue = mock(SelectableMessageQueue.class);
        when(messageQueue.getBrokerName()).thenReturn("mockBroker");

        List<SendResult> sendResultList = this.producerProcessor.sendMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            PRODUCER_GROUP,
            messageExtList,
            3000
        ).get();

        assertNotNull(sendResultList);
        TransactionId transactionId = TransactionId.decode(sendResultList.get(0).getTransactionId());
        assertNotNull(transactionId);
        assertEquals(txId, transactionId.getBrokerTransactionId());
        assertEquals("mockBroker", transactionId.getBrokerName());

        SendMessageRequestHeader requestHeader = requestHeaderArgumentCaptor.getValue();
        assertEquals(PRODUCER_GROUP, requestHeader.getProducerGroup());
        assertEquals(TOPIC, requestHeader.getTopic());
    }

    @Test
    public void testSendRetryMessage() throws Throwable {
        String txId = MessageClientIDSetter.createUniqID();
        String msgId = MessageClientIDSetter.createUniqID();

        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setTransactionId(txId);
        sendResult.setMsgId(msgId);
        ArgumentCaptor<SendMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(SendMessageRequestHeader.class);
        when(this.messageService.sendMessage(any(), any(), any(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(sendResult)));

        List<MessageExt> messageExtList = new ArrayList<>();
        MessageExt messageExt = createMessageExt(MixAll.getRetryTopic(CONSUMER_GROUP), "tag", 0, 0);
        messageExt.setSysFlag(MessageSysFlag.TRANSACTION_PREPARED_TYPE);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_RECONSUME_TIME, "1");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_MAX_RECONSUME_TIMES, "16");
        messageExtList.add(messageExt);
        SelectableMessageQueue messageQueue = mock(SelectableMessageQueue.class);
        when(messageQueue.getBrokerName()).thenReturn("mockBroker");

        List<SendResult> sendResultList = this.producerProcessor.sendMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            PRODUCER_GROUP,
            messageExtList,
            3000
        ).get();

        assertNotNull(sendResultList);
        TransactionId transactionId = TransactionId.decode(sendResultList.get(0).getTransactionId());
        assertNotNull(transactionId);
        assertEquals(txId, transactionId.getBrokerTransactionId());
        assertEquals("mockBroker", transactionId.getBrokerName());

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

        MessageExt messageExt = createMessageExt(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP), "", 16, 3000);
        RemotingCommand remotingCommand = this.producerProcessor.forwardMessageToDeadLetterQueue(
            createContext(),
            ReceiptHandle.create(messageExt),
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
}