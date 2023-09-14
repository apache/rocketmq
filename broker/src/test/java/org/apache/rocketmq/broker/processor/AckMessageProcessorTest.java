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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;

import static org.apache.rocketmq.broker.processor.PullMessageProcessorTest.createConsumerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AckMessageProcessorTest {
    private AckMessageProcessor ackMessageProcessor;
    @Mock
    private PopMessageProcessor popMessageProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private DefaultMessageStore messageStore;
    @Mock
    private Channel channel;

    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ClientChannelInfo clientInfo;
    @Mock
    private Broker2Client broker2Client;

    private static final long MIN_OFFSET_IN_QUEUE = 100;
    private static final long MAX_OFFSET_IN_QUEUE = 999;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        clientInfo = new ClientChannelInfo(channel, "127.0.0.1", LanguageCode.JAVA, 0);
        brokerController.setMessageStore(messageStore);
        Field field = BrokerController.class.getDeclaredField("broker2Client");
        field.setAccessible(true);
        field.set(brokerController, broker2Client);
        EscapeBridge escapeBridge = new EscapeBridge(brokerController);
        Mockito.when(brokerController.getEscapeBridge()).thenReturn(escapeBridge);
        Channel mockChannel = mock(Channel.class);
        when(handlerContext.channel()).thenReturn(mockChannel);
        brokerController.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig());
        ConsumerData consumerData = createConsumerData(group, topic);
        brokerController.getConsumerManager().registerConsumer(
                consumerData.getGroupName(),
                clientInfo,
                consumerData.getConsumeType(),
                consumerData.getMessageModel(),
                consumerData.getConsumeFromWhere(),
                consumerData.getSubscriptionDataSet(),
                false);
        ackMessageProcessor = new AckMessageProcessor(brokerController);

        when(messageStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(MIN_OFFSET_IN_QUEUE);
        when(messageStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(MAX_OFFSET_IN_QUEUE);

        when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
    }

    @Test
    public void testProcessRequest_Success() throws RemotingCommandException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        PopBufferMergeService popBufferMergeService = mock(PopBufferMergeService.class);
        when(popBufferMergeService.addAk(anyInt(), any())).thenReturn(false);
        when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);

        int queueId = 0;
        long queueOffset = 0;
        long popTime = System.currentTimeMillis() - 1_000;
        long invisibleTime = 30_000;
        int reviveQid = 0;
        String brokerName = "test_broker";
        String extraInfo = ExtraInfoUtil.buildExtraInfo(queueOffset, popTime, invisibleTime, reviveQid,
                topic, brokerName, queueId) + MessageConst.KEY_SEPARATOR + queueOffset;
        AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(0);
        requestHeader.setOffset(MIN_OFFSET_IN_QUEUE + 1);
        requestHeader.setConsumerGroup(group);
        requestHeader.setExtraInfo(extraInfo);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand responseToReturn = ackMessageProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseToReturn.getOpaque()).isEqualTo(request.getOpaque());
    }

    @Test
    public void testProcessRequest_WrongRequestCode() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.MESSAGE_ILLEGAL);
        assertThat(response.getRemark()).isEqualTo("AckMessageProcessor failed to process RequestCode: " + RequestCode.SEND_MESSAGE);
    }

    @Test
    public void testSingleAck_TopicCheck() throws RemotingCommandException {
        AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
        requestHeader.setTopic("wrongTopic");
        requestHeader.setQueueId(0);
        requestHeader.setOffset(0L);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
        assertThat(response.getRemark()).contains("not exist, apply first");
    }

    @Test
    public void testSingleAck_QueueCheck() throws RemotingCommandException {
        {
            int qId = -1;
            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(qId);
            requestHeader.setOffset(0L);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.MESSAGE_ILLEGAL);
            assertThat(response.getRemark()).contains("queueId[" + qId + "] is illegal");
        }

        {
            int qId = 17;
            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(qId);
            requestHeader.setOffset(0L);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.MESSAGE_ILLEGAL);
            assertThat(response.getRemark()).contains("queueId[" + qId + "] is illegal");
        }
    }

    @Test
    public void testSingleAck_OffsetCheck() throws RemotingCommandException {
        {
            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            requestHeader.setOffset(MIN_OFFSET_IN_QUEUE - 1);
            //requestHeader.setOffset(maxOffsetInQueue + 1);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.NO_MESSAGE);
            assertThat(response.getRemark()).contains("offset is illegal");
        }

        {
            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            //requestHeader.setOffset(minOffsetInQueue - 1);
            requestHeader.setOffset(MAX_OFFSET_IN_QUEUE + 1);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.NO_MESSAGE);
            assertThat(response.getRemark()).contains("offset is illegal");
        }
    }

    @Test
    public void testBatchAck_NoMessage() throws RemotingCommandException {
        {
            //reqBody == null
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.NO_MESSAGE);
        }

        {
            //reqBody.getAcks() == null
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
            BatchAckMessageRequestBody reqBody = new BatchAckMessageRequestBody();
            request.setBody(reqBody.encode());
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.NO_MESSAGE);
        }

        {
            //reqBody.getAcks().isEmpty()
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
            BatchAckMessageRequestBody reqBody = new BatchAckMessageRequestBody();
            reqBody.setAcks(new ArrayList<>());
            request.setBody(reqBody.encode());
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.NO_MESSAGE);
        }
    }

    @Test
    public void testSingleAck_appendAck() throws RemotingCommandException {
        {
            // buffer addAk OK
            PopBufferMergeService popBufferMergeService = mock(PopBufferMergeService.class);
            when(popBufferMergeService.addAk(anyInt(), any())).thenReturn(true);
            when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);

            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            long ackOffset = MIN_OFFSET_IN_QUEUE + 10;
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            requestHeader.setOffset(ackOffset);
            requestHeader.setConsumerGroup(MixAll.DEFAULT_CONSUMER_GROUP);
            requestHeader.setExtraInfo("64 1666860736757 60000 4 0 broker-a 0 " + ackOffset);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);

            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        {
            // buffer addAk fail
            PopBufferMergeService popBufferMergeService = mock(PopBufferMergeService.class);
            when(popBufferMergeService.addAk(anyInt(), any())).thenReturn(false);
            when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);
            // store putMessage OK
            PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, null);
            when(messageStore.putMessage(any())).thenReturn(putMessageResult);

            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            long ackOffset = MIN_OFFSET_IN_QUEUE + 10;
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            requestHeader.setOffset(ackOffset);
            requestHeader.setConsumerGroup(MixAll.DEFAULT_CONSUMER_GROUP);
            requestHeader.setExtraInfo("64 1666860736757 60000 4 0 broker-a 0 " + ackOffset);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);

            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testBatchAck_appendAck() throws RemotingCommandException {
        {
            // buffer addAk OK
            PopBufferMergeService popBufferMergeService = mock(PopBufferMergeService.class);
            when(popBufferMergeService.addAk(anyInt(), any())).thenReturn(true);
            when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);

            BatchAck bAck1 = new BatchAck();
            bAck1.setConsumerGroup(MixAll.DEFAULT_CONSUMER_GROUP);
            bAck1.setTopic(topic);
            bAck1.setStartOffset(MIN_OFFSET_IN_QUEUE);
            bAck1.setBitSet(new BitSet());
            bAck1.getBitSet().set(1);
            bAck1.setRetry("0");

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
            BatchAckMessageRequestBody reqBody = new BatchAckMessageRequestBody();
            reqBody.setAcks(Collections.singletonList(bAck1));
            request.setBody(reqBody.encode());
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);

            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }

        {
            // buffer addAk fail
            PopBufferMergeService popBufferMergeService = mock(PopBufferMergeService.class);
            when(popBufferMergeService.addAk(anyInt(), any())).thenReturn(false);
            when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);
            // store putMessage OK
            PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, null);
            when(messageStore.putMessage(any())).thenReturn(putMessageResult);

            BatchAck bAck1 = new BatchAck();
            bAck1.setConsumerGroup(MixAll.DEFAULT_CONSUMER_GROUP);
            bAck1.setTopic(topic);
            bAck1.setStartOffset(MIN_OFFSET_IN_QUEUE);
            bAck1.setBitSet(new BitSet());
            bAck1.getBitSet().set(1);
            bAck1.setRetry("0");

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
            BatchAckMessageRequestBody reqBody = new BatchAckMessageRequestBody();
            reqBody.setAcks(Arrays.asList(bAck1));
            request.setBody(reqBody.encode());
            request.makeCustomHeaderToNet();
            RemotingCommand response = ackMessageProcessor.processRequest(handlerContext, request);

            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

}
