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

package org.apache.rocketmq.test.proxy;

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.rpc.Code;
import io.grpc.Channel;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.configuration.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.GrpcMessagingProcessor;
import org.apache.rocketmq.proxy.grpc.service.LocalGrpcService;
import org.apache.rocketmq.test.base.GrpcBaseTest;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID;
import static org.apache.rocketmq.proxy.configuration.ConfigurationManager.RMQ_PROXY_HOME;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalGrpcTest extends GrpcBaseTest {
    private MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;

    @Before
    public void setUp() throws Exception {
        String mockProxyHome = "/mock/rmq/proxy/home";
        URL mockProxyHomeURL = getClass().getClassLoader().getResource("rmq-proxy-home");
        if (mockProxyHomeURL != null) {
            mockProxyHome = mockProxyHomeURL.toURI().getPath();
        }
        System.setProperty(RMQ_PROXY_HOME, mockProxyHome);
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        GrpcMessagingProcessor processor = new GrpcMessagingProcessor(new LocalGrpcService(brokerController1));
        ConfigurationManager.getProxyConfig().setGrpcServerPort(8082);
        Channel channel = setUpServer(processor, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);
        blockingStub = MessagingServiceGrpc.newBlockingStub(channel);
    }

    @Test
    public void testSendReceiveMessage() {
        String group = "group";
        SendMessageResponse sendResponse = blockingStub.sendMessage(SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(broker1Name)
                    .build())
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId(createUniqID())
                    .setPartitionId(0)
                    .build())
                .setBody(ByteString.copyFromUtf8("123"))
                .build())
            .build());
        assertThat(sendResponse.getCommon()
            .getStatus()
            .getCode()).isEqualTo(Code.OK.getNumber());
        String messageId = sendResponse.getMessageId();
        ReceiveMessageResponse receiveResponse = blockingStub.withDeadlineAfter(3, TimeUnit.SECONDS)
            .receiveMessage(ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder()
                    .setName(group)
                    .build())
                .setPartition(Partition.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setName(broker1Name)
                        .build())
                    .setId(0)
                    .build())
                .setBatchSize(16)
                .setInvisibleDuration(Duration.newBuilder()
                    .setSeconds(3)
                    .build())
                .setInitializationTimestamp(Timestamp.newBuilder()
                    .setSeconds(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
                    .build())
                .build());
        assertThat(receiveResponse.getCommon()
            .getStatus()
            .getCode()).isEqualTo(Code.OK.getNumber());
        assertThat(receiveResponse.getMessagesCount()).isEqualTo(1);
        assertThat(receiveResponse.getMessages(0)
            .getSystemAttribute()
            .getMessageId()).isEqualTo(messageId);
    }
}
