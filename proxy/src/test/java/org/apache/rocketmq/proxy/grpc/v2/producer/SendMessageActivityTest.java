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
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class SendMessageActivityTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";

    private SendMessageActivity sendMessageActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.sendMessageActivity = new SendMessageActivity(this.messagingProcessor, this.grpcClientSettingsManager);
    }

    @Test
    public void sendMessage() throws Exception {
        String msgId = MessageClientIDSetter.createUniqID();

        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId(msgId);
        when(this.messagingProcessor.sendMessage(any(), any(), anyString(), any()))
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
        long deliveryTime = System.currentTimeMillis();
        String msgId = MessageClientIDSetter.createUniqID();

        MessageExt messageExt = this.sendMessageActivity.buildMessage(null,
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
        assertEquals(String.valueOf(deliveryTime), messageExt.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
    }

    @Test
    public void testTxMessage() {
        String msgId = MessageClientIDSetter.createUniqID();

        MessageExt messageExt = this.sendMessageActivity.buildMessage(null,
            Lists.newArrayList(
                Message.newBuilder()
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
                    .build()
            ),
            Resource.newBuilder().setName(TOPIC).build()).get(0);

        assertEquals(MessageClientIDSetter.getUniqID(messageExt), msgId);
        assertEquals(MessageSysFlag.TRANSACTION_PREPARED_TYPE | MessageSysFlag.COMPRESSED_FLAG, messageExt.getSysFlag());
    }
}