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

import io.netty.channel.local.LocalChannel;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.receipt.DefaultReceiptHandleManager;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReceiptHandleProcessorTest extends InitConfigTest {

    @Mock
    protected MessagingProcessor messagingProcessor;
    @Mock
    protected ServiceManager serviceManager;
    @Mock
    protected ConsumerManager consumerManager;
    @Mock
    protected MetadataService metadataService;

    private static final ProxyContext PROXY_CONTEXT = ProxyContext.create();
    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String TOPIC = "topic";
    private static final String BROKER_NAME = "broker";
    private static final int QUEUE_ID = 1;
    private static final String MESSAGE_ID = "messageId";
    private static final long OFFSET = 123L;
    private static final long INVISIBLE_TIME = 60000L;
    private static final int RECONSUME_TIMES = 1;
    private static final String MSG_ID = MessageClientIDSetter.createUniqID();
    private MessageReceiptHandle messageReceiptHandle;

    private ReceiptHandleProcessor receiptHandleProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        when(serviceManager.getConsumerManager()).thenReturn(consumerManager);
        when(serviceManager.getMetadataService()).thenReturn(metadataService);
        this.receiptHandleProcessor = new ReceiptHandleProcessor(this.messagingProcessor, this.serviceManager);
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        String receiptHandle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis() - INVISIBLE_TIME + config.getRenewAheadTimeMillis() - 5)
            .invisibleTime(INVISIBLE_TIME)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(BROKER_NAME)
            .queueId(QUEUE_ID)
            .offset(OFFSET)
            .commitLogOffset(0L)
            .build().encode();
        PROXY_CONTEXT.withVal(ContextVariable.CLIENT_ID, "channel-id");
        PROXY_CONTEXT.withVal(ContextVariable.CHANNEL, new LocalChannel());
        messageReceiptHandle = new MessageReceiptHandle(CONSUMER_GROUP, TOPIC, QUEUE_ID, receiptHandle, MESSAGE_ID, OFFSET,
            RECONSUME_TIMES);
    }

    @Test
    public void testStart() throws Exception {
        Mockito.when(consumerManager.findChannel(Mockito.eq(CONSUMER_GROUP), Mockito.eq(PROXY_CONTEXT.getChannel()))).thenReturn(Mockito.mock(ClientChannelInfo.class));
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        ackResult.setExtraInfo(messageReceiptHandle.getReceiptHandleStr());
        Mockito.when(messagingProcessor.changeInvisibleTime(
                Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC),
                Mockito.eq(ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills()), Mockito.eq(null)))
            .thenReturn(CompletableFuture.completedFuture(ackResult));

        receiptHandleProcessor.addReceiptHandle(PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, MSG_ID, messageReceiptHandle);
        try {
            receiptHandleProcessor.start();
            Mockito.verify(messagingProcessor, Mockito.timeout(10000).times(1))
                .changeInvisibleTime(Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                    Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC), Mockito.eq(ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills()), Mockito.eq(null));
        } finally {
            receiptHandleProcessor.removeReceiptHandle(PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, MSG_ID,
                messageReceiptHandle.getReceiptHandleStr());
            receiptHandleProcessor.shutdown();
        }
    }

    @Test
    public void testRenewWithBatchChangeInvisibleTime() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        Mockito.when(consumerManager.findChannel(Mockito.eq(CONSUMER_GROUP), Mockito.eq(PROXY_CONTEXT.getChannel())))
            .thenReturn(Mockito.mock(ClientChannelInfo.class));

        String anotherMsgId = MessageClientIDSetter.createUniqID();
        MessageReceiptHandle anotherHandle = new MessageReceiptHandle(
            CONSUMER_GROUP,
            TOPIC,
            QUEUE_ID,
            ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(System.currentTimeMillis() - INVISIBLE_TIME
                    + ConfigurationManager.getProxyConfig().getRenewAheadTimeMillis() - 5)
                .invisibleTime(INVISIBLE_TIME)
                .reviveQueueId(1)
                .topicType(ReceiptHandle.NORMAL_TOPIC)
                .brokerName(BROKER_NAME)
                .queueId(QUEUE_ID)
                .offset(OFFSET + 1)
                .commitLogOffset(0L)
                .build().encode(),
            anotherMsgId,
            OFFSET + 1,
            RECONSUME_TIMES);

        ArgumentCaptor<List> handleMessageListCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.doAnswer((Answer<CompletableFuture<List<BatchChangeInvisibleTimeResult>>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            List<BatchChangeInvisibleTimeResult> results = new ArrayList<>(handleMessageList.size());
            for (ReceiptHandleMessage handleMessage : handleMessageList) {
                AckResult ackResult = new AckResult();
                ackResult.setStatus(AckStatus.OK);
                ackResult.setExtraInfo(handleMessage.getReceiptHandle().encode());
                results.add(new BatchChangeInvisibleTimeResult(handleMessage, ackResult));
            }
            return CompletableFuture.completedFuture(results);
        }).when(messagingProcessor).batchChangeInvisibleTime(
            Mockito.any(), handleMessageListCaptor.capture(), Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC),
            Mockito.eq(ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills()),
            Mockito.eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), Mockito.eq(false));

        receiptHandleProcessor.addReceiptHandle(
            PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, MSG_ID, messageReceiptHandle);
        receiptHandleProcessor.addReceiptHandle(
            PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, anotherMsgId, anotherHandle);

        Method method = DefaultReceiptHandleManager.class.getDeclaredMethod("scheduleRenewTask");
        method.setAccessible(true);
        method.invoke(receiptHandleProcessor.receiptHandleManager);

        Mockito.verify(messagingProcessor, Mockito.timeout(10000).times(1)).batchChangeInvisibleTime(
            Mockito.any(), Mockito.anyList(), Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC),
            Mockito.eq(ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills()),
            Mockito.eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), Mockito.eq(false));
        assertEquals(2, handleMessageListCaptor.getValue().size());
        Mockito.verify(messagingProcessor, Mockito.never()).changeInvisibleTime(
            Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
            Mockito.anyLong(), Mockito.any());
    }

    @Test
    public void testClientOfflineClearGroupWithBatchChangeInvisibleTime() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        ArgumentCaptor<ConsumerIdsChangeListener> listenerArgumentCaptor =
            ArgumentCaptor.forClass(ConsumerIdsChangeListener.class);
        Mockito.verify(consumerManager).appendConsumerIdsChangeListener(listenerArgumentCaptor.capture());

        String anotherMsgId = MessageClientIDSetter.createUniqID();
        MessageReceiptHandle anotherHandle = new MessageReceiptHandle(
            CONSUMER_GROUP,
            TOPIC,
            QUEUE_ID,
            ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(System.currentTimeMillis() - INVISIBLE_TIME
                    + ConfigurationManager.getProxyConfig().getRenewAheadTimeMillis() - 5)
                .invisibleTime(INVISIBLE_TIME)
                .reviveQueueId(1)
                .topicType(ReceiptHandle.NORMAL_TOPIC)
                .brokerName(BROKER_NAME)
                .queueId(QUEUE_ID)
                .offset(OFFSET + 1)
                .commitLogOffset(0L)
                .build().encode(),
            anotherMsgId,
            OFFSET + 1,
            RECONSUME_TIMES);

        ArgumentCaptor<List> handleMessageListCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.doAnswer((Answer<CompletableFuture<List<BatchChangeInvisibleTimeResult>>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            List<BatchChangeInvisibleTimeResult> results = new ArrayList<>(handleMessageList.size());
            for (ReceiptHandleMessage handleMessage : handleMessageList) {
                AckResult ackResult = new AckResult();
                ackResult.setStatus(AckStatus.OK);
                results.add(new BatchChangeInvisibleTimeResult(handleMessage, ackResult));
            }
            return CompletableFuture.completedFuture(results);
        }).when(messagingProcessor).batchChangeInvisibleTime(
            Mockito.any(), handleMessageListCaptor.capture(), Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC),
            Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()),
            Mockito.eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), Mockito.eq(false));

        receiptHandleProcessor.addReceiptHandle(
            PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, MSG_ID, messageReceiptHandle);
        receiptHandleProcessor.addReceiptHandle(
            PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, anotherMsgId, anotherHandle);

        listenerArgumentCaptor.getValue().handle(ConsumerGroupEvent.CLIENT_UNREGISTER, CONSUMER_GROUP,
            new ClientChannelInfo(PROXY_CONTEXT.getChannel(), "clientId", LanguageCode.JAVA, 0));

        Mockito.verify(messagingProcessor, Mockito.timeout(10000).times(1)).batchChangeInvisibleTime(
            Mockito.any(), Mockito.anyList(), Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC),
            Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()),
            Mockito.eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), Mockito.eq(false));
        assertEquals(2, handleMessageListCaptor.getValue().size());
        Mockito.verify(messagingProcessor, Mockito.never()).changeInvisibleTime(
            Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
            Mockito.anyLong(), Mockito.any());
    }

    @Test
    public void testClientOfflineClearGroupWithSingleHandleUseSingleChangeInvisibleTime() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        ArgumentCaptor<ConsumerIdsChangeListener> listenerArgumentCaptor =
            ArgumentCaptor.forClass(ConsumerIdsChangeListener.class);
        Mockito.verify(consumerManager).appendConsumerIdsChangeListener(listenerArgumentCaptor.capture());

        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        Mockito.when(messagingProcessor.changeInvisibleTime(
                Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
                Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC),
                Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()), Mockito.eq(null)))
            .thenReturn(CompletableFuture.completedFuture(ackResult));

        receiptHandleProcessor.addReceiptHandle(
            PROXY_CONTEXT, PROXY_CONTEXT.getChannel(), CONSUMER_GROUP, MSG_ID, messageReceiptHandle);

        listenerArgumentCaptor.getValue().handle(ConsumerGroupEvent.CLIENT_UNREGISTER, CONSUMER_GROUP,
            new ClientChannelInfo(PROXY_CONTEXT.getChannel(), "clientId", LanguageCode.JAVA, 0));

        Mockito.verify(messagingProcessor, Mockito.timeout(10000).times(1)).changeInvisibleTime(
            Mockito.any(ProxyContext.class), Mockito.any(ReceiptHandle.class), Mockito.eq(MESSAGE_ID),
            Mockito.eq(CONSUMER_GROUP), Mockito.eq(TOPIC),
            Mockito.eq(ConfigurationManager.getProxyConfig().getInvisibleTimeMillisWhenClear()), Mockito.eq(null));
        Mockito.verify(messagingProcessor, Mockito.never()).batchChangeInvisibleTime(
            Mockito.any(), Mockito.anyList(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
            Mockito.anyLong(), Mockito.anyBoolean());
    }

}
