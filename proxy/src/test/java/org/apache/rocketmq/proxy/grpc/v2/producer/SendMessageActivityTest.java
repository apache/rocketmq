/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.apache.rocketmq.proxy.service.route.TopicRouteService.buildPenalizerByMQFaultStrategy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SendMessageActivityTest extends BaseActivityTest {

    protected static final String BROKER_NAME = "broker";
    protected static final String BROKER_NAME2 = "broker2";
    protected static final String CLUSTER_NAME = "cluster";
    protected static final String BROKER_ADDR = "127.0.0.1:10911";
    protected static final String BROKER_ADDR2 = "127.0.0.1:10912";
    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";
    MQFaultStrategy mqFaultStrategy;

    private SendMessageActivity sendMessageActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.sendMessageActivity = new SendMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void sendMessage() throws Exception {
        String msgId = MessageClientIDSetter.createUniqID();

        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId(msgId);
        when(this.messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(sendResult)));

        SendMessageResponse response = this.sendMessageActivity.sendMessage(
            createContext(),
            SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setName(TOPIC)
                        .build())
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageId(msgId)
                        .setQueueId(0)
                        .setMessageType(MessageType.NORMAL)
                        .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                        .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                        .build())
                    .setBody(ByteString.copyFromUtf8("123"))
                    .build())
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(msgId, response.getEntries(0).getMessageId());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSendFifoBatchInOneInvocation() throws Exception {
        String firstMessageId = MessageClientIDSetter.createUniqID();
        String secondMessageId = MessageClientIDSetter.createUniqID();
        String messageGroup = "group";
        SendResult sendResult = new SendResult(SendStatus.SEND_OK, null, null, null, 10);
        when(this.messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(sendResult)));

        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(createMessage(firstMessageId, MessageType.FIFO, messageGroup, 16))
            .addMessages(createMessage(secondMessageId, MessageType.FIFO, messageGroup, 16))
            .build();
        SendMessageResponse response = this.sendMessageActivity.sendMessage(createContext(), request).get();

        ArgumentCaptor<List<org.apache.rocketmq.common.message.Message>> messageListCaptor =
            ArgumentCaptor.forClass(List.class);
        verify(this.messagingProcessor, times(1)).sendMessage(any(), any(), anyString(), anyInt(),
            messageListCaptor.capture());
        assertEquals(2, messageListCaptor.getValue().size());
        assertEquals(2, response.getEntriesCount());
        assertEquals(firstMessageId, response.getEntries(0).getMessageId());
        assertEquals(secondMessageId, response.getEntries(1).getMessageId());
        assertEquals(10, response.getEntries(0).getOffset());
        assertEquals(11, response.getEntries(1).getOffset());
    }

    @Test
    public void testRejectFifoBatchWithDifferentMessageGroups() {
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(createMessage(MessageClientIDSetter.createUniqID(), MessageType.FIFO, "group-a", 16))
            .addMessages(createMessage(MessageClientIDSetter.createUniqID(), MessageType.FIFO, "group-b", 16))
            .build();

        ExecutionException exception = assertThrows(ExecutionException.class,
            () -> this.sendMessageActivity.sendMessage(createContext(), request).get());
        GrpcProxyException cause = (GrpcProxyException) exception.getCause();
        assertEquals(Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, cause.getCode());
    }

    @Test
    public void testRejectBatchWithDifferentLiteTopics() {
        Message firstMessage = withLiteTopic(createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.LITE, "", 16), "lite-a");
        Message secondMessage = withLiteTopic(createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.LITE, "", 16), "lite-b");

        ExecutionException exception = assertThrows(ExecutionException.class,
            () -> this.sendMessageActivity.sendMessage(createContext(), SendMessageRequest.newBuilder()
                .addMessages(firstMessage)
                .addMessages(secondMessage)
                .build()).get());
        assertEquals(Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
            ((GrpcProxyException) exception.getCause()).getCode());
        verify(this.messagingProcessor, never()).sendMessage(any(), any(), anyString(), anyInt(), any());
    }

    @Test
    public void testRejectBatchWithMessageTypePropertyConflict() {
        assertBatchMessageTypeConflict(createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "group", 16));

        Message priorityMessage = createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 16);
        priorityMessage = priorityMessage.toBuilder()
            .setSystemProperties(priorityMessage.getSystemProperties().toBuilder().setPriority(1))
            .build();
        assertBatchMessageTypeConflict(priorityMessage);

        assertBatchMessageTypeConflict(withLiteTopic(createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 16), "lite-topic"));

        Message delayMessage = createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 16);
        delayMessage = delayMessage.toBuilder()
            .setSystemProperties(delayMessage.getSystemProperties().toBuilder()
                .setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis() + 1000)))
            .build();
        assertBatchMessageTypeConflict(delayMessage);

        verify(this.messagingProcessor, never()).sendMessage(any(), any(), anyString(), anyInt(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSendBatchWithSameLiteTopic() throws Exception {
        String liteTopic = "same-lite-topic";
        Message firstMessage = withLiteTopic(createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.LITE, "", 16), liteTopic);
        Message secondMessage = withLiteTopic(createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.LITE, "", 16), liteTopic);
        SendResult sendResult = new SendResult(SendStatus.SEND_OK, null, null, null, 0);
        when(this.messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(Lists.newArrayList(sendResult)));

        this.sendMessageActivity.sendMessage(createContext(), SendMessageRequest.newBuilder()
            .addMessages(firstMessage)
            .addMessages(secondMessage)
            .build()).get();

        ArgumentCaptor<List<org.apache.rocketmq.common.message.Message>> messageListCaptor =
            ArgumentCaptor.forClass(List.class);
        verify(this.messagingProcessor).sendMessage(any(), any(), anyString(), anyInt(), messageListCaptor.capture());
        assertEquals(liteTopic,
            messageListCaptor.getValue().get(0).getProperty(MessageConst.PROPERTY_LITE_TOPIC));
        assertEquals(liteTopic,
            messageListCaptor.getValue().get(1).getProperty(MessageConst.PROPERTY_LITE_TOPIC));
    }

    @Test
    public void testRejectCompressedBatch() {
        Message compressedMessage = createMessage(
            MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 16);
        compressedMessage = compressedMessage.toBuilder()
            .setSystemProperties(compressedMessage.getSystemProperties().toBuilder()
                .setBodyEncoding(Encoding.GZIP))
            .build();
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(compressedMessage)
            .addMessages(createMessage(MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 16))
            .build();

        ExecutionException exception = assertThrows(ExecutionException.class,
            () -> this.sendMessageActivity.sendMessage(createContext(), request).get());
        GrpcProxyException cause = (GrpcProxyException) exception.getCause();
        assertEquals(Code.MESSAGE_CORRUPTED, cause.getCode());
        verify(this.messagingProcessor, never()).sendMessage(any(), any(), anyString(), anyInt(), any());
    }

    @Test
    public void testRejectBatchWhoseEncodedBodyExceedsLimit() {
        int previousMaxMessageSize = ConfigurationManager.getProxyConfig().getMaxMessageSize();
        ConfigurationManager.getProxyConfig().setMaxMessageSize(80);
        try {
            SendMessageRequest request = SendMessageRequest.newBuilder()
                .addMessages(createMessage(MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 30))
                .addMessages(createMessage(MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 30))
                .build();

            ExecutionException exception = assertThrows(ExecutionException.class,
                () -> this.sendMessageActivity.sendMessage(createContext(), request).get());
            assertEquals(Code.MESSAGE_BODY_TOO_LARGE, ((GrpcProxyException) exception.getCause()).getCode());
            verify(this.messagingProcessor, never()).sendMessage(any(), any(), anyString(), anyInt(), any());
        } finally {
            ConfigurationManager.getProxyConfig().setMaxMessageSize(previousMaxMessageSize);
        }
    }

    @Test
    public void testRejectBatchWhoseMessageCountExceedsLimit() {
        int previousMaxMessageCount = ConfigurationManager.getProxyConfig().getBatchSendMaxMsgNum();
        ConfigurationManager.getProxyConfig().setBatchSendMaxMsgNum(1);
        try {
            SendMessageRequest request = SendMessageRequest.newBuilder()
                .addMessages(createMessage(MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 1))
                .addMessages(createMessage(MessageClientIDSetter.createUniqID(), MessageType.NORMAL, "", 1))
                .build();

            ExecutionException exception = assertThrows(ExecutionException.class,
                () -> this.sendMessageActivity.sendMessage(createContext(), request).get());
            assertEquals(Code.MESSAGE_CORRUPTED, ((GrpcProxyException) exception.getCause()).getCode());
            verify(this.messagingProcessor, never()).sendMessage(any(), any(), anyString(), anyInt(), any());
        } finally {
            ConfigurationManager.getProxyConfig().setBatchSendMaxMsgNum(previousMaxMessageCount);
        }
    }

    private Message createMessage(String messageId, MessageType messageType, String messageGroup, int bodySize) {
        return Message.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setSystemProperties(SystemProperties.newBuilder()
                .setMessageId(messageId)
                .setMessageType(messageType)
                .setMessageGroup(messageGroup)
                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                .build())
            .setBody(ByteString.copyFrom(new byte[bodySize]))
            .build();
    }

    private Message withLiteTopic(Message message, String liteTopic) {
        return message.toBuilder()
            .setSystemProperties(message.getSystemProperties().toBuilder().setLiteTopic(liteTopic))
            .build();
    }

    private void assertBatchMessageTypeConflict(Message message) {
        ExecutionException exception = assertThrows(ExecutionException.class,
            () -> this.sendMessageActivity.sendMessage(createContext(), SendMessageRequest.newBuilder()
                .addMessages(message)
                .addMessages(message)
                .build()).get());
        assertEquals(Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
            ((GrpcProxyException) exception.getCause()).getCode());
    }

    @Test
    public void testConvertToSendMessageResponse() {
        {
            SendMessageResponse response = this.sendMessageActivity.convertToSendMessageResponse(
                ProxyContext.create(),
                SendMessageRequest.newBuilder().build(),
                Lists.newArrayList(new SendResult(SendStatus.FLUSH_DISK_TIMEOUT, null, null, null, 0))
            );
            assertEquals(Code.MASTER_PERSISTENCE_TIMEOUT, response.getStatus().getCode());
            assertEquals(Code.MASTER_PERSISTENCE_TIMEOUT, response.getEntries(0).getStatus().getCode());
        }

        {
            SendMessageResponse response = this.sendMessageActivity.convertToSendMessageResponse(
                ProxyContext.create(),
                SendMessageRequest.newBuilder().build(),
                Lists.newArrayList(new SendResult(SendStatus.FLUSH_SLAVE_TIMEOUT, null, null, null, 0))
            );
            assertEquals(Code.SLAVE_PERSISTENCE_TIMEOUT, response.getStatus().getCode());
            assertEquals(Code.SLAVE_PERSISTENCE_TIMEOUT, response.getEntries(0).getStatus().getCode());
        }

        {
            SendMessageResponse response = this.sendMessageActivity.convertToSendMessageResponse(
                ProxyContext.create(),
                SendMessageRequest.newBuilder().build(),
                Lists.newArrayList(new SendResult(SendStatus.SLAVE_NOT_AVAILABLE, null, null, null, 0))
            );
            assertEquals(Code.HA_NOT_AVAILABLE, response.getStatus().getCode());
            assertEquals(Code.HA_NOT_AVAILABLE, response.getEntries(0).getStatus().getCode());
        }

        {
            SendMessageResponse response = this.sendMessageActivity.convertToSendMessageResponse(
                ProxyContext.create(),
                SendMessageRequest.newBuilder().build(),
                Lists.newArrayList(new SendResult(SendStatus.SEND_OK, null, null, null, 0))
            );
            assertEquals(Code.OK, response.getStatus().getCode());
            assertEquals(Code.OK, response.getEntries(0).getStatus().getCode());
        }

        {
            SendMessageResponse response = this.sendMessageActivity.convertToSendMessageResponse(
                ProxyContext.create(),
                SendMessageRequest.newBuilder().build(),
                Lists.newArrayList(
                    new SendResult(SendStatus.SEND_OK, null, null, null, 0),
                    new SendResult(SendStatus.SLAVE_NOT_AVAILABLE, null, null, null, 0)
                )
            );
            assertEquals(Code.MULTIPLE_RESULTS, response.getStatus().getCode());
        }
    }

    @Test(expected = GrpcProxyException.class)
    public void testValidateMessagesWithDifferentTopics() {
        this.sendMessageActivity.validateMessageList(
            Lists.newArrayList(
                Message.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setName(TOPIC)
                        .build())
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageId(MessageClientIDSetter.createUniqID())
                        .setQueueId(0)
                        .setMessageType(MessageType.NORMAL)
                        .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                        .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                        .build())
                    .setBody(ByteString.copyFromUtf8("123"))
                    .build(),
                Message.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setName(TOPIC + 2)
                        .build())
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageId(MessageClientIDSetter.createUniqID())
                        .setQueueId(0)
                        .setMessageType(MessageType.NORMAL)
                        .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                        .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                        .build())
                    .setBody(ByteString.copyFromUtf8("123"))
                    .build()
            ));
    }

    @Test
    public void testBuildMessage() {
        long deliveryTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        ConfigurationManager.getProxyConfig().setMessageDelayLevel("1s 5s");
        ConfigurationManager.getProxyConfig().initData();
        String msgId = MessageClientIDSetter.createUniqID();

        org.apache.rocketmq.common.message.Message messageExt = this.sendMessageActivity.buildMessage(null,
            Lists.newArrayList(
                Message.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setName(TOPIC)
                        .build())
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageId(msgId)
                        .setQueueId(0)
                        .setMessageType(MessageType.DELAY)
                        .setDeliveryTimestamp(Timestamps.fromMillis(deliveryTime))
                        .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                        .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                        .build())
                    .setBody(ByteString.copyFromUtf8("123"))
                    .build()
            ),
            Resource.newBuilder().setName(TOPIC).build()).get(0);

        assertEquals(MessageClientIDSetter.getUniqID(messageExt), msgId);
        assertEquals(deliveryTime, Long.parseLong(messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS)));
    }

    @Test
    public void testBuildMessageWithLiteTopic() {
        String msgId = MessageClientIDSetter.createUniqID();
        String liteTopic = "build-test-lite-topic";
        String topic = "build-test-topic";

        org.apache.rocketmq.common.message.Message messageExt = this.sendMessageActivity.buildMessage(
            ProxyContext.create(),
            Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(msgId)
                    .setQueueId(0)
                    .setMessageType(MessageType.LITE)
                    .setLiteTopic(liteTopic)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .setBody(ByteString.copyFromUtf8("test body"))
                .build(),
            "test-producer-group"
        );

        assertEquals(liteTopic, messageExt.getProperty(MessageConst.PROPERTY_LITE_TOPIC));
        assertNull(messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH));
    }

    @Test
    public void testTxMessage() {
        String msgId = MessageClientIDSetter.createUniqID();

        Message message = Message.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName(TOPIC)
                .build())
            .setSystemProperties(SystemProperties.newBuilder()
                .setMessageId(msgId)
                .setQueueId(0)
                .setMessageType(MessageType.TRANSACTION)
                .setOrphanedTransactionRecoveryDuration(Durations.fromSeconds(30))
                .setBodyEncoding(Encoding.GZIP)
                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                .build())
            .setBody(ByteString.copyFromUtf8("123"))
            .build();
        org.apache.rocketmq.common.message.Message messageExt = this.sendMessageActivity.buildMessage(null,
            Lists.newArrayList(
                message
            ),
            Resource.newBuilder().setName(TOPIC).build()).get(0);

        assertEquals(MessageClientIDSetter.getUniqID(messageExt), msgId);
        assertEquals(MessageSysFlag.TRANSACTION_PREPARED_TYPE | MessageSysFlag.COMPRESSED_FLAG, sendMessageActivity.buildSysFlag(message));
    }

    @Test
    public void testPriorityMessage() {
        String msgId = MessageClientIDSetter.createUniqID();
        Message message = Message.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName(TOPIC)
                .build())
            .setSystemProperties(SystemProperties.newBuilder()
                .setMessageId(msgId)
                .setQueueId(0)
                .setMessageType(MessageType.PRIORITY)
                .setPriority(5)
                .setBodyEncoding(Encoding.GZIP)
                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                .build())
            .setBody(ByteString.copyFromUtf8("123"))
            .build();
        org.apache.rocketmq.common.message.Message messageExt = this.sendMessageActivity.buildMessage(null,
            Lists.newArrayList(
                message
            ),
            Resource.newBuilder().setName(TOPIC).build()).get(0);

        assertEquals(MessageClientIDSetter.getUniqID(messageExt), msgId);
        assertEquals(5, messageExt.getPriority());
    }

    @Test
    public void testSendOrderMessageQueueSelector() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        QueueData queueData = new QueueData();
        BrokerData brokerData = new BrokerData();
        queueData.setBrokerName(BROKER_NAME);
        queueData.setWriteQueueNums(8);
        queueData.setPerm(PermName.PERM_WRITE);
        topicRouteData.setQueueDatas(Lists.newArrayList(queueData));
        brokerData.setCluster(CLUSTER_NAME);
        brokerData.setBrokerName(BROKER_NAME);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, BROKER_ADDR);
        brokerData.setBrokerAddrs(brokerAddrs);
        topicRouteData.setBrokerDatas(Lists.newArrayList(brokerData));

        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData, null);
        SendMessageActivity.SendMessageQueueSelector selector1 = new SendMessageActivity.SendMessageQueueSelector(
            SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder()
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageGroup(String.valueOf(1))
                        .build())
                    .build())
                .build()
        );

        TopicRouteService topicRouteService = mock(TopicRouteService.class);
        MQFaultStrategy mqFaultStrategy = mock(MQFaultStrategy.class);
        when(topicRouteService.getAllMessageQueueView(any(), any())).thenReturn(messageQueueView);
        when(topicRouteService.getMqFaultStrategy()).thenReturn(mqFaultStrategy);
        when(mqFaultStrategy.isSendLatencyFaultEnable()).thenReturn(false);

        SendMessageActivity.SendMessageQueueSelector selector2 = new SendMessageActivity.SendMessageQueueSelector(
            SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder()
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageGroup(String.valueOf(1))
                        .build())
                    .build())
                .addMessages(Message.newBuilder()
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageGroup(String.valueOf(1))
                        .build())
                    .build())
                .build()
        );

        SendMessageActivity.SendMessageQueueSelector selector3 = new SendMessageActivity.SendMessageQueueSelector(
            SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder()
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageGroup(String.valueOf(2))
                        .build())
                    .build())
                .build()
        );

        assertEquals(selector1.select(ProxyContext.create(), messageQueueView), selector2.select(ProxyContext.create(), messageQueueView));
        assertNotEquals(selector1.select(ProxyContext.create(), messageQueueView), selector3.select(ProxyContext.create(), messageQueueView));
    }

    @Test
    public void testSendNormalMessageQueueSelector() {
        TopicRouteData topicRouteData = new TopicRouteData();
        QueueData queueData = new QueueData();
        BrokerData brokerData = new BrokerData();
        queueData.setBrokerName(BROKER_NAME);
        queueData.setWriteQueueNums(2);
        queueData.setPerm(PermName.PERM_WRITE);
        topicRouteData.setQueueDatas(Lists.newArrayList(queueData));
        brokerData.setCluster(CLUSTER_NAME);
        brokerData.setBrokerName(BROKER_NAME);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, BROKER_ADDR);
        brokerData.setBrokerAddrs(brokerAddrs);
        topicRouteData.setBrokerDatas(Lists.newArrayList(brokerData));


        SendMessageActivity.SendMessageQueueSelector selector = new SendMessageActivity.SendMessageQueueSelector(
            SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder().build())
                .build()
        );
        TopicRouteService topicRouteService = mock(TopicRouteService.class);
        MQFaultStrategy mqFaultStrategy = mock(MQFaultStrategy.class);
        when(topicRouteService.getMqFaultStrategy()).thenReturn(mqFaultStrategy);
        when(mqFaultStrategy.isSendLatencyFaultEnable()).thenReturn(false);
        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData, null);

        AddressableMessageQueue firstSelect = selector.select(ProxyContext.create(), messageQueueView);
        AddressableMessageQueue secondSelect = selector.select(ProxyContext.create(), messageQueueView);
        AddressableMessageQueue thirdSelect = selector.select(ProxyContext.create(), messageQueueView);

        assertEquals(firstSelect, thirdSelect);
        assertNotEquals(firstSelect, secondSelect);
    }

    @Test
    public void testSendNormalMessageQueueSelectorPipeLine() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        int queueNums = 2;

        QueueData queueData = createQueueData(BROKER_NAME, queueNums);
        QueueData queueData2 = createQueueData(BROKER_NAME2, queueNums);
        topicRouteData.setQueueDatas(Lists.newArrayList(queueData,queueData2));


        BrokerData brokerData = createBrokerData(CLUSTER_NAME, BROKER_NAME, BROKER_ADDR);
        BrokerData brokerData2 = createBrokerData(CLUSTER_NAME, BROKER_NAME2, BROKER_ADDR2);
        topicRouteData.setBrokerDatas(Lists.newArrayList(brokerData, brokerData2));

        SendMessageActivity.SendMessageQueueSelector selector = new SendMessageActivity.SendMessageQueueSelector(
                SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder().build())
                        .build()
        );

        ClientConfig cc = new ClientConfig();
        this.mqFaultStrategy = new MQFaultStrategy(cc, null, null);
        mqFaultStrategy.setSendLatencyFaultEnable(true);
        mqFaultStrategy.updateFaultItem(BROKER_NAME2, 1000, true, true);
        mqFaultStrategy.updateFaultItem(BROKER_NAME, 1000, true, false);

        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData, buildPenalizerByMQFaultStrategy(mqFaultStrategy));

        AddressableMessageQueue firstSelect = selector.select(ProxyContext.create(), messageQueueView);
        assertEquals(firstSelect.getBrokerName(), BROKER_NAME2);

        mqFaultStrategy.updateFaultItem(BROKER_NAME2, 1000, true, false);
        mqFaultStrategy.updateFaultItem(BROKER_NAME, 1000, true, true);
        AddressableMessageQueue secondSelect = selector.select(ProxyContext.create(), messageQueueView);
        assertEquals(secondSelect.getBrokerName(), BROKER_NAME);
    }
    @Test
    public void testParameterValidate() {
        // too large message body
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[4 * 1024 * 1024 + 1]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.MESSAGE_BODY_TOO_LARGE, e.getCode());
                throw e;
            }
        });

        // black tag
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setTag("   ")
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_TAG, e.getCode());
                throw e;
            }
        });

        // tag with '|'
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setTag("|")
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_TAG, e.getCode());
                throw e;
            }
        });

        // tag with \t
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setTag("\t")
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_TAG, e.getCode());
                throw e;
            }
        });

        // blank message key
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .addKeys("  ")
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_KEY, e.getCode());
                throw e;
            }
        });

        // blank message with \t
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .addKeys("\t")
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_KEY, e.getCode());
                throw e;
            }
        });

        // blank message group
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageGroup("  ")
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_GROUP, e.getCode());
                throw e;
            }
        });

        // long message group
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageGroup(createStr(ConfigurationManager.getProxyConfig().getMaxMessageGroupSize() + 1))
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_GROUP, e.getCode());
                throw e;
            }
        });

        // message group with \t
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageGroup("\t")
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_GROUP, e.getCode());
                throw e;
            }
        });

        // too large message property
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .putUserProperties("key", createStr(16 * 1024 + 1))
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.MESSAGE_PROPERTIES_TOO_LARGE, e.getCode());
                throw e;
            }
        });

        // too large message property
        assertThrows(GrpcProxyException.class, () -> {
            Map<String, String> p = new HashMap<>();
            for (int i = 0; i <= ConfigurationManager.getProxyConfig().getUserPropertyMaxNum(); i++) {
                p.put(String.valueOf(i), String.valueOf(i));
            }
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .putAllUserProperties(p)
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.MESSAGE_PROPERTIES_TOO_LARGE, e.getCode());
                throw e;
            }
        });

        // set system properties
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .putUserProperties(MessageConst.PROPERTY_TRACE_SWITCH, "false")
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, e.getCode());
                throw e;
            }
        });

        // set the key of user property with control character
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .putUserProperties("\u0000", "hello")
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, e.getCode());
                throw e;
            }
        });

        // set the value of user property with control character
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("msgId")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .putUserProperties("p", "\u0000")
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, e.getCode());
                throw e;
            }
        });

        // empty message id
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId(" ")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_MESSAGE_ID, e.getCode());
                throw e;
            }
        });

        // delay time
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("id")
                                .setDeliveryTimestamp(
                                    Timestamps.fromMillis(System.currentTimeMillis() + Duration.ofDays(1).toMillis() + Duration.ofSeconds(10).toMillis()))
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.ILLEGAL_DELIVERY_TIME, e.getCode());
                throw e;
            }
        });

        // transaction message cannot be delay message
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("id")
                                .setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis() + Duration.ofSeconds(5).toMillis()))
                                .setQueueId(0)
                                .setMessageType(MessageType.TRANSACTION)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.BAD_REQUEST, e.getCode());
                assertEquals("transaction message cannot set delivery timestamp", e.getMessage());
                throw e;
            }
        });

        // transactionRecoverySecond
        assertThrows(GrpcProxyException.class, () -> {
            try {
                this.sendMessageActivity.sendMessage(
                    createContext(),
                    SendMessageRequest.newBuilder()
                        .addMessages(Message.newBuilder()
                            .setTopic(Resource.newBuilder()
                                .setName(TOPIC)
                                .build())
                            .setSystemProperties(SystemProperties.newBuilder()
                                .setMessageId("id")
                                .setQueueId(0)
                                .setMessageType(MessageType.NORMAL)
                                .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                                .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                                .setOrphanedTransactionRecoveryDuration(Durations.fromHours(2))
                                .setMessageType(MessageType.TRANSACTION)
                                .build())
                            .setBody(ByteString.copyFrom(new byte[3]))
                            .build())
                        .build()
                ).get();
            } catch (ExecutionException t) {
                GrpcProxyException e = (GrpcProxyException) t.getCause();
                assertEquals(Code.BAD_REQUEST, e.getCode());
                throw e;
            }
        });
    }

    private static String createStr(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append("a");
        }
        return sb.toString();
    }

    private static QueueData createQueueData(String brokerName, int writeQueueNums) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(writeQueueNums);
        queueData.setPerm(PermName.PERM_WRITE);
        return queueData;
    }

    private static BrokerData createBrokerData(String clusterName, String brokerName, String brokerAddrs) {
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster(clusterName);
        brokerData.setBrokerName(brokerName);
        HashMap<Long, String> brokerAddrsMap = new HashMap<>();
        brokerAddrsMap.put(MixAll.MASTER_ID, brokerAddrs);
        brokerData.setBrokerAddrs(brokerAddrsMap);

        return brokerData;
    }
}
