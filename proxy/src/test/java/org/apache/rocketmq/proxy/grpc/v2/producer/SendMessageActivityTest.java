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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class SendMessageActivityTest extends BaseActivityTest {

    protected static final String BROKER_NAME = "broker";
    protected static final String CLUSTER_NAME = "cluster";
    protected static final String BROKER_ADDR = "127.0.0.1:10911";
    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";

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
                        .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
                        .build())
                    .setBody(ByteString.copyFromUtf8("123"))
                    .build())
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(msgId, response.getEntries(0).getMessageId());
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
    public void testBuildErrorMessage() {
        this.sendMessageActivity.buildMessage(null,
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
                        .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                        .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
                        .build())
                    .setBody(ByteString.copyFromUtf8("123"))
                    .build()
            ),
            Resource.newBuilder().setName(TOPIC).build());
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
                        .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
                        .build())
                    .setBody(ByteString.copyFromUtf8("123"))
                    .build()
            ),
            Resource.newBuilder().setName(TOPIC).build()).get(0);

        assertEquals(MessageClientIDSetter.getUniqID(messageExt), msgId);
        assertEquals(String.valueOf(2), messageExt.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL));
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
                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
    public void testSendOrderMessageQueueSelector() {
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

        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData);
        SendMessageActivity.SendMessageQueueSelector selector1 = new SendMessageActivity.SendMessageQueueSelector(
            SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder()
                    .setSystemProperties(SystemProperties.newBuilder()
                        .setMessageGroup(String.valueOf(1))
                        .build())
                    .build())
                .build()
        );

        SendMessageActivity.SendMessageQueueSelector selector2 = new SendMessageActivity.SendMessageQueueSelector(
            SendMessageRequest.newBuilder()
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

        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData);
        SendMessageActivity.SendMessageQueueSelector selector = new SendMessageActivity.SendMessageQueueSelector(
            SendMessageRequest.newBuilder()
                .addMessages(Message.newBuilder().build())
                .build()
        );

        AddressableMessageQueue firstSelect = selector.select(ProxyContext.create(), messageQueueView);
        AddressableMessageQueue secondSelect = selector.select(ProxyContext.create(), messageQueueView);
        AddressableMessageQueue thirdSelect = selector.select(ProxyContext.create(), messageQueueView);

        assertEquals(firstSelect, thirdSelect);
        assertNotEquals(firstSelect, secondSelect);
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
                                .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
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
}