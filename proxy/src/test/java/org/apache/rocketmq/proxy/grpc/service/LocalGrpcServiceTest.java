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

package org.apache.rocketmq.proxy.grpc.service;

import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import io.grpc.Context;
import io.grpc.Metadata;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.InterceptorConstants;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class LocalGrpcServiceTest extends InitConfigAndLoggerTest {
    private LocalGrpcService localGrpcService;
    @Mock
    private SendMessageProcessor sendMessageProcessorMock;
    @Mock
    private PopMessageProcessor popMessageProcessorMock;
    @Mock
    private BrokerController brokerControllerMock;

    private Metadata metadata;

    @Before
    public void setUp() throws Exception {
        super.before();
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        Mockito.when(brokerControllerMock.getPopMessageProcessor()).thenReturn(popMessageProcessorMock);
        localGrpcService = new LocalGrpcService(brokerControllerMock);
        metadata = new Metadata();
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "1.1.1.1");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "0.0.0.0");
        metadata.put(InterceptorConstants.LANGUAGE, "JAVA");
    }

    @Test
    public void testHeartbeat() {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ClientManageProcessor clientManageProcessorMock = Mockito.mock(ClientManageProcessor.class);
        Mockito.when(clientManageProcessorMock.heartBeat(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        Mockito.when(brokerControllerMock.getClientManageProcessor()).thenReturn(clientManageProcessorMock);
        HeartbeatRequest request = HeartbeatRequest.newBuilder().getDefaultInstanceForType();
        CompletableFuture<HeartbeatResponse> grpcFuture = localGrpcService.heartbeat(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        grpcFuture.thenAccept(r -> {
            assertThat(r.getCommon().getStatus().getCode())
                .isEqualTo(Code.OK.getNumber());
            assertThat(r.getCommon().getStatus().getMessage())
                .isEqualTo(null);
        });
    }

    @Test
    public void testSendMessageError() throws RemotingCommandException {
        String remark = "store putMessage return null";
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, remark);
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(response);
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(future);
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        grpcFuture.thenAccept(r -> {
            assertThat(r.getCommon().getStatus().getCode())
                .isEqualTo(Code.INTERNAL.getNumber());
            assertThat(r.getCommon().getStatus().getMessage())
                .isEqualTo(remark);
        });
    }

    @Test
    public void testSendMessageWriteAndFlush() throws RemotingCommandException {
        CompletableFuture<RemotingCommand> future = CompletableFuture.completedFuture(null);
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(future);
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        grpcFuture.thenAccept(r -> assertThat(r).isNull());
    }

    @Test
    public void testSendMessageWithException() throws RemotingCommandException {
        Mockito.when(sendMessageProcessorMock.asyncProcessRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenThrow(new RemotingCommandException("test"));
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current().withValue(InterceptorConstants.METADATA, metadata).attach(), request);
        grpcFuture.thenAccept(r -> assertThat(r).isNull()).exceptionally(e -> {
            assertThat(e).isInstanceOf(RemotingCommandException.class);
            return null;
        });
    }

    @Test
    public void testReceiveMessageSuccess() throws Exception {
        long invisibleTime = 1000L;
        String topic = "topic";
        byte[] body = "123".getBytes(StandardCharsets.UTF_8);
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setQueueOffset(0L);
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setBody(body);
        messageExt.putUserProperty("key", "value");
        PopMessageResponseHeader responseHeader = new PopMessageResponseHeader();
        responseHeader.setInvisibleTime(invisibleTime);
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, responseHeader);
        remotingCommand.setBody(MessageDecoder.encode(messageExt, true));
        remotingCommand.makeCustomHeaderToNet();
        Mockito.when(popMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(remotingCommand);
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().getDefaultInstanceForType();
        CompletableFuture<ReceiveMessageResponse> grpcFuture = localGrpcService.receiveMessage(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .withDeadlineAfter(20, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("test")))
                .attach(), request);
        grpcFuture.thenAccept(r -> {
            assertThat(r.getCommon().getStatus().getCode()).isEqualTo(Code.OK.getNumber());
            assertThat(r.getMessagesCount()).isEqualTo(0);
            assertThat(Durations.toMillis(r.getInvisibleDuration())).isEqualTo(invisibleTime);
            assertThat(Converter.getResourceNameWithNamespace(r.getMessages(0).getTopic())).isEqualTo(topic);
            assertThat(r.getMessages(0).getBody().toByteArray()).isEqualTo(body);
        });
    }

    @Test
    public void testReceiveMessageSuccessWriteAndFlush() throws Exception {
        Mockito.when(popMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(null);
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().getDefaultInstanceForType();
        CompletableFuture<ReceiveMessageResponse> grpcFuture = localGrpcService.receiveMessage(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .withDeadlineAfter(20, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryImpl("test")))
                .attach(), request);
        grpcFuture.thenAccept(r -> assertThat(r).isNull());
    }
}