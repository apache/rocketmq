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
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.producer.RecallMessageHandle;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class RecallMessageProcessorTest {
    private static final String TOPIC = "topic";
    private static final String BROKER_NAME = "brokerName";

    private RecallMessageProcessor recallMessageProcessor;
    @Mock
    private BrokerConfig brokerConfig;
    @Mock
    private BrokerController brokerController;
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private MessageStoreConfig messageStoreConfig;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private MessageStore messageStore;
    @Mock
    private BrokerStatsManager brokerStatsManager;
    @Mock
    private Channel channel;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerConfig.getBrokerName()).thenReturn(BROKER_NAME);
        when(brokerController.getBrokerStatsManager()).thenReturn(brokerStatsManager);
        when(handlerContext.channel()).thenReturn(channel);
        recallMessageProcessor = new RecallMessageProcessor(brokerController);
    }

    @Test
    public void testBuildMessage() {
        String timestampStr = String.valueOf(System.currentTimeMillis());
        String id = "id";
        RecallMessageHandle.HandleV1 handle = new RecallMessageHandle.HandleV1(TOPIC, "brokerName", timestampStr, id);
        MessageExtBrokerInner msg =
            recallMessageProcessor.buildMessage(handlerContext, new RecallMessageRequestHeader(), handle);

        Assert.assertEquals(TOPIC, msg.getTopic());
        Map<String, String> properties = MessageDecoder.string2messageProperties(msg.getPropertiesString());
        Assert.assertEquals(timestampStr, properties.get(MessageConst.PROPERTY_TIMER_DELIVER_MS));
        Assert.assertEquals(id, properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        Assert.assertEquals(id, properties.get(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY));
    }

    @Test
    public void testHandlePutMessageResult() {
        MessageExt message = new MessageExt();
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "id");
        RemotingCommand response = RemotingCommand.createResponseCommand(RecallMessageResponseHeader.class);
        recallMessageProcessor.handlePutMessageResult(null, null, response, message, handlerContext, 0L);
        Assert.assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());

        List<PutMessageStatus> okStatus = Arrays.asList(PutMessageStatus.PUT_OK, PutMessageStatus.FLUSH_DISK_TIMEOUT,
            PutMessageStatus.FLUSH_SLAVE_TIMEOUT, PutMessageStatus.SLAVE_NOT_AVAILABLE);

        for (PutMessageStatus status : PutMessageStatus.values()) {
            PutMessageResult putMessageResult =
                new PutMessageResult(status, new AppendMessageResult(AppendMessageStatus.PUT_OK));
            recallMessageProcessor.handlePutMessageResult(putMessageResult, null, response, message, handlerContext, 0L);
            if (okStatus.contains(status)) {
                Assert.assertEquals(ResponseCode.SUCCESS, response.getCode());
                RecallMessageResponseHeader responseHeader = (RecallMessageResponseHeader) response.readCustomHeader();
                Assert.assertEquals("id", responseHeader.getMsgId());
            } else {
                Assert.assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
            }
        }
    }

    @Test
    public void testProcessRequest_invalidStatus() throws RemotingCommandException {
        RemotingCommand request = mockRequest(0, TOPIC, TOPIC, "id", BROKER_NAME);
        RemotingCommand response;

        // role slave
        when(messageStoreConfig.getBrokerRole()).thenReturn(BrokerRole.SLAVE);
        response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.SLAVE_NOT_AVAILABLE, response.getCode());

        // not reach startTimestamp
        when(messageStoreConfig.getBrokerRole()).thenReturn(BrokerRole.SYNC_MASTER);
        when(messageStore.now()).thenReturn(0L);
        when(brokerConfig.getStartAcceptSendRequestTimeStamp()).thenReturn(System.currentTimeMillis());
        response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.SERVICE_NOT_AVAILABLE, response.getCode());
    }

    @Test
    public void testProcessRequest_notWriteable() throws RemotingCommandException {
        when(brokerConfig.getBrokerPermission()).thenReturn(4);
        when(brokerConfig.isAllowRecallWhenBrokerNotWriteable()).thenReturn(false);
        RemotingCommand request = mockRequest(0, TOPIC, TOPIC, "id", BROKER_NAME);
        RemotingCommand response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.SERVICE_NOT_AVAILABLE, response.getCode());
    }

    @Test
    public void testProcessRequest_topicNotFound_or_notMatch() throws RemotingCommandException {
        when(brokerConfig.getBrokerPermission()).thenReturn(6);
        RemotingCommand request;
        RemotingCommand response;

        // not found
        request = mockRequest(0, TOPIC, TOPIC, "id", BROKER_NAME);
        response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.TOPIC_NOT_EXIST, response.getCode());

        // not match
        when(topicConfigManager.selectTopicConfig(TOPIC)).thenReturn(new TopicConfig(TOPIC));
        request = mockRequest(0, TOPIC, "anotherTopic", "id", BROKER_NAME);
        response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.ILLEGAL_OPERATION, response.getCode());
    }

    @Test
    public void testProcessRequest_brokerNameNotMatch() throws RemotingCommandException {
        when(brokerConfig.getBrokerPermission()).thenReturn(6);
        when(topicConfigManager.selectTopicConfig(TOPIC)).thenReturn(new TopicConfig(TOPIC));

        RemotingCommand request = mockRequest(0, TOPIC, "anotherTopic", "id", BROKER_NAME + "_other");
        RemotingCommand response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.ILLEGAL_OPERATION, response.getCode());
    }

    @Test
    public void testProcessRequest_timestampInvalid() throws RemotingCommandException {
        when(brokerConfig.getBrokerPermission()).thenReturn(6);
        when(topicConfigManager.selectTopicConfig(TOPIC)).thenReturn(new TopicConfig(TOPIC));
        RemotingCommand request;
        RemotingCommand response;

        // past timestamp
        request = mockRequest(0, TOPIC, TOPIC, "id", BROKER_NAME);
        response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.ILLEGAL_OPERATION, response.getCode());

        // timestamp overflow
        when(messageStoreConfig.getTimerMaxDelaySec()).thenReturn(86400);
        request = mockRequest(System.currentTimeMillis() + 86400 * 2 * 1000, TOPIC, TOPIC, "id", BROKER_NAME);
        response = recallMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(ResponseCode.ILLEGAL_OPERATION, response.getCode());
    }

    @Test
    public void testProcessRequest_success() throws RemotingCommandException {
        when(brokerConfig.getBrokerPermission()).thenReturn(6);
        when(topicConfigManager.selectTopicConfig(TOPIC)).thenReturn(new TopicConfig(TOPIC));
        when(messageStoreConfig.getTimerMaxDelaySec()).thenReturn(86400);
        when(messageStore.putMessage(any())).thenReturn(
            new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));

        String msgId = "msgId";
        RemotingCommand request = mockRequest(System.currentTimeMillis() + 90 * 1000, TOPIC, TOPIC, msgId, BROKER_NAME);
        RemotingCommand response = recallMessageProcessor.processRequest(handlerContext, request);
        RecallMessageResponseHeader responseHeader = (RecallMessageResponseHeader) response.readCustomHeader();
        Assert.assertEquals(ResponseCode.SUCCESS, response.getCode());
        Assert.assertEquals(msgId, responseHeader.getMsgId());
        verify(messageStore, times(1)).putMessage(any());
    }

    private RemotingCommand mockRequest(long timestamp, String requestTopic, String handleTopic,
        String msgId, String brokerName) {
        String handle =
            RecallMessageHandle.HandleV1.buildHandle(handleTopic, brokerName, String.valueOf(timestamp), msgId);
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        requestHeader.setProducerGroup("group");
        requestHeader.setTopic(requestTopic);
        requestHeader.setRecallHandle(handle);
        requestHeader.setBrokerName(brokerName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RECALL_MESSAGE, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }
}
