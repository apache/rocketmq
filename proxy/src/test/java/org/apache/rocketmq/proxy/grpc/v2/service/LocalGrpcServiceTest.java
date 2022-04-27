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

package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SystemProperties;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageResult;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.proxy.common.TelemetryCommandManager;
import org.apache.rocketmq.proxy.common.TelemetryCommandRecord;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.connector.transaction.TransactionId;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(MockitoJUnitRunner.class)
public class LocalGrpcServiceTest extends InitConfigAndLoggerTest {
    private LocalGrpcService localGrpcService;
    @Mock
    private SendMessageProcessor sendMessageProcessorMock;
    @Mock
    private PopMessageProcessor popMessageProcessorMock;
    @Mock
    private PullMessageProcessor pullMessageProcessorMock;
    @Mock
    private BrokerController brokerControllerMock;
    @Mock
    private ConsumerManager consumerManagerMock;
    @Mock
    private ProducerManager producerManagerMock;
    @Mock
    private TopicConfigManager topicConfigManagerMock;

    @Mock
    private TelemetryCommandManager telemetryCommandManager;

    StreamObserver<ReceiveMessageResponse> receiveStreamObserver = Mockito.mock(ServerCallStreamObserver.class);

    private Metadata metadata;

    private StreamObserver<TelemetryCommand> streamObserver;

    @Before
    public void setUp() throws Throwable {
        super.before();
        ConfigurationManager.getProxyConfig().setNameSrvAddr("1.1.1.1");
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        Mockito.when(brokerControllerMock.getPopMessageProcessor()).thenReturn(popMessageProcessorMock);
        Mockito.when(brokerControllerMock.getBrokerConfig()).thenReturn(new BrokerConfig());
        Mockito.when(brokerControllerMock.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        Mockito.when(brokerControllerMock.getTopicConfigManager()).thenReturn(topicConfigManagerMock);
        Mockito.when(topicConfigManagerMock.selectTopicConfig(Mockito.anyString())).thenReturn(new TopicConfig("topic", 8, 8));
        Mockito.doNothing().when(consumerManagerMock).appendConsumerIdsChangeListener(Mockito.any(ConsumerIdsChangeListener.class));
        Mockito.doNothing().when(producerManagerMock).appendProducerChangeListener(Mockito.any(ProducerChangeListener.class));
        Mockito.when(brokerControllerMock.getConsumerManager()).thenReturn(consumerManagerMock);
        Mockito.when(brokerControllerMock.getProducerManager()).thenReturn(producerManagerMock);
        localGrpcService = new LocalGrpcService(brokerControllerMock, telemetryCommandManager);
        metadata = new Metadata();
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "1.1.1.1");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "0.0.0.0");
        metadata.put(InterceptorConstants.LANGUAGE, "JAVA");
        metadata.put(InterceptorConstants.CLIENT_ID, "client-id");
        Context.current().withValue(InterceptorConstants.METADATA, metadata).attach();
        streamObserver = localGrpcService.telemetry(Context.current(), new StreamObserver<TelemetryCommand>() {
            @Override public void onNext(TelemetryCommand value) {
            }

            @Override public void onError(Throwable t) {
            }

            @Override public void onCompleted() {
            }
        });
        streamObserver.onNext(TelemetryCommand.newBuilder()
            .setSettings(Settings.newBuilder()
                .setBackoffPolicy(RetryPolicy.newBuilder()
                    .setMaxAttempts(3).build())
                .setSubscription(Subscription.newBuilder().build())
                .build()).build());
    }

    @Test
    public void testHeartbeatProducerData() throws Exception {
        streamObserver.onNext(TelemetryCommand.newBuilder()
            .setSettings(Settings.newBuilder()
                .setPublishing(Publishing.newBuilder()
                    .addTopics(Resource.newBuilder()
                        .setName("topic")
                        .build())
                    .build())
                .setClientType(ClientType.PRODUCER)
                .build())
            .build());
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ClientManageProcessor clientManageProcessorMock = Mockito.mock(ClientManageProcessor.class);
        Mockito.when(clientManageProcessorMock.heartBeat(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        Mockito.when(brokerControllerMock.getClientManageProcessor()).thenReturn(clientManageProcessorMock);
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .build();
        CompletableFuture<HeartbeatResponse> grpcFuture = localGrpcService.heartbeat(Context.current(), request);
        HeartbeatResponse r =  grpcFuture.get();
        assertThat(r.getStatus().getCode())
            .isEqualTo(Code.OK);
    }

    @Test
    public void testHeartbeatConsumerData() throws Exception {
        streamObserver.onNext(TelemetryCommand.newBuilder()
            .setSettings(Settings.newBuilder()
                .setClientType(ClientType.PUSH_CONSUMER).build())
            .build());
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        ClientManageProcessor clientManageProcessorMock = Mockito.mock(ClientManageProcessor.class);
        Mockito.when(clientManageProcessorMock.heartBeat(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        Mockito.when(brokerControllerMock.getClientManageProcessor()).thenReturn(clientManageProcessorMock);
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .build();
        CompletableFuture<HeartbeatResponse> grpcFuture = localGrpcService.heartbeat(Context.current(), request);
        HeartbeatResponse r =  grpcFuture.get();
        assertThat(r.getStatus().getCode())
            .isEqualTo(Code.OK);
    }

    @Test
    public void testSendMessageError() throws Exception {
        String remark = "store putMessage return null";
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, remark);
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(0, Message.newBuilder()
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current(), request);
        SendMessageResponse r = grpcFuture.get();
        assertThat(r.getStatus().getCode())
            .isEqualTo(Code.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testSendMessageWriteAndFlush() throws Exception {
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(null);
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(0, Message.newBuilder()
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current(), request);
        assertThat(grpcFuture.isDone()).isFalse();
    }

    @Test
    public void testSendMessageBatchWithWriteAndFlush() throws Exception {
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(null);
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .addMessages(Message.newBuilder()
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("124")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current(), request);
        assertThat(grpcFuture.isDone()).isFalse();
    }

    @Test
    public void testSendMessageWithException() throws Exception {
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenThrow(new RemotingCommandException("test"));
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(0, Message.newBuilder()
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("123")
                    .build())
                .build())
            .build();

        CompletableFuture<SendMessageResponse> grpcFuture = localGrpcService.sendMessage(
            Context.current(), request);
        assertThatThrownBy(() -> {
            try {
                grpcFuture.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }).isInstanceOf(RemotingCommandException.class);
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
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);
        remotingCommand.setBody(MessageDecoder.encode(messageExt, true));
        remotingCommand.makeCustomHeaderToNet();
        Mockito.when(popMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(remotingCommand);
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder()
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .build())
            .build();
        ReceiveMessageResponse receiveMessageResponse1 = ReceiveMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
            .build();
        Message message = GrpcConverter.buildMessage(messageExt);
        ReceiveMessageResponse receiveMessageResponse2 = ReceiveMessageResponse.newBuilder()
            .setMessage(message.toBuilder()
                .setSystemProperties(
                    message.getSystemProperties()
                    .toBuilder()
                    .setReceiptHandle("0 0 1000 0 0 zhouxiang_MBP16 0 0 0")
                    .build())
                .build())
            .build();
        Mockito.doNothing().when(receiveStreamObserver).onNext(Mockito.any());
        localGrpcService.receiveMessage(Context.current().withDeadlineAfter(20, TimeUnit.SECONDS,
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("test"))), request, receiveStreamObserver);
        ArgumentCaptor<ReceiveMessageResponse> argument = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        Mockito.verify(receiveStreamObserver, Mockito.times(2)).onNext(argument.capture());
        assertThat(argument.getAllValues().get(0)).isEqualTo(receiveMessageResponse1);
        assertThat(argument.getAllValues().get(1)).isEqualTo(receiveMessageResponse2);
    }

    @Test
    public void testAckMessage() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        AckMessageProcessor ackMessageProcessorMock = Mockito.mock(AckMessageProcessor.class);
        Mockito.when(brokerControllerMock.getAckMessageProcessor()).thenReturn(ackMessageProcessorMock);
        Mockito.when(ackMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        AckMessageRequest request = AckMessageRequest.newBuilder()
            .addEntries(
                AckMessageEntry.newBuilder()
                    .setReceiptHandle(ReceiptHandle.builder()
                        .startOffset(0L)
                        .retrieveTime(System.currentTimeMillis())
                        .invisibleTime(1000L)
                        .reviveQueueId(0)
                        .topicType("topic")
                        .brokerName("brokerName")
                        .queueId(0)
                        .offset(0L)
                        .build().encode())
                    .build())
            .build();
        CompletableFuture<AckMessageResponse> grpcFuture = localGrpcService.ackMessage(Context.current(), request);
        AckMessageResponse r = grpcFuture.get();
        assertThat(r.getStatus().getCode()).isEqualTo(Code.OK);
    }

    @Test
    public void testNackMessage() throws Exception {
        ChangeInvisibleTimeResponseHeader responseHeader = new ChangeInvisibleTimeResponseHeader();
        responseHeader.setInvisibleTime(1000L);
        responseHeader.setPopTime(0L);
        responseHeader.setReviveQid(0);
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);

        ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor = Mockito.mock(ChangeInvisibleTimeProcessor.class);
        Mockito.when(brokerControllerMock.getChangeInvisibleTimeProcessor()).thenReturn(changeInvisibleTimeProcessor);
        Mockito.when(changeInvisibleTimeProcessor.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        NackMessageRequest request = NackMessageRequest.newBuilder().setReceiptHandle(
            ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(0L)
                .invisibleTime(1000L)
                .nextVisibleTime(1000L)
                .reviveQueueId(0)
                .topicType("topic")
                .brokerName("brokerName")
                .queueId(0)
                .offset(0L)
                .build().encode()
        ).build();
        CompletableFuture<NackMessageResponse> grpcFuture = localGrpcService.nackMessage(Context.current(), request);
        NackMessageResponse r = grpcFuture.get();
        assertThat(r.getStatus().getCode()).isEqualTo(Code.OK);
    }

    @Test
    public void testNackMessageWhenDLQ() throws Exception {
        ConsumerSendMsgBackRequestHeader responseHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);

        SendMessageProcessor sendMessageProcessor = Mockito.mock(SendMessageProcessor.class);
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessor);
        Mockito.when(sendMessageProcessor.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        NackMessageRequest request = NackMessageRequest.newBuilder()
            .setDeliveryAttempt(3)
            .setReceiptHandle(
                ReceiptHandle.builder()
                    .startOffset(0L)
                    .retrieveTime(0L)
                    .invisibleTime(1000L)
                    .nextVisibleTime(1000L)
                    .reviveQueueId(0)
                    .topicType("topic")
                    .brokerName("brokerName")
                    .queueId(0)
                    .offset(0L)
                    .build().encode()
            ).build();
        CompletableFuture<NackMessageResponse> grpcFuture = localGrpcService.nackMessage(
            Context.current(), request);
        NackMessageResponse r = grpcFuture.get();
        assertThat(r.getStatus().getCode()).isEqualTo(Code.OK);
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(ChannelHandlerContext.class),
            Mockito.argThat(argument -> argument.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK)))
            .thenReturn(response);
        ForwardMessageToDeadLetterQueueRequest request = ForwardMessageToDeadLetterQueueRequest.newBuilder()
            .setReceiptHandle(ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(0L)
                .invisibleTime(1000L)
                .nextVisibleTime(1000L)
                .reviveQueueId(0)
                .topicType("topic")
                .brokerName("brokerName")
                .queueId(0)
                .offset(0L)
                .build().encode())
            .build();
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> grpcFuture = localGrpcService.forwardMessageToDeadLetterQueue(
            Context.current(), request);
        ForwardMessageToDeadLetterQueueResponse r = grpcFuture.get();
        assertThat(r.getStatus().getCode()).isEqualTo(Code.OK);
    }

    @Test
    public void testEndTransaction() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);

        EndTransactionProcessor endTransactionProcessor = Mockito.mock(EndTransactionProcessor.class);
        Mockito.when(brokerControllerMock.getEndTransactionProcessor()).thenReturn(endTransactionProcessor);
        Mockito.when(endTransactionProcessor.processRequest(Mockito.any(ChannelHandlerContext.class),
            Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        EndTransactionRequest request = EndTransactionRequest.newBuilder()
            .setMessageId("123")
            .setTransactionId(TransactionId.genByBrokerTransactionId(
                new InetSocketAddress("0.0.0.0", 80), "123", 123, 123
                ).getProxyTransactionId()
            )
            .build();
        CompletableFuture<EndTransactionResponse> grpcFuture = localGrpcService.endTransaction(
            Context.current(), request);
        EndTransactionResponse r = grpcFuture.get();
        assertThat(r.getStatus().getCode()).isEqualTo(Code.OK);
    }

    @Test
    public void testReportThreadStackTrace() throws Exception {
        int opaque = 1;
        String nonce = "123";
        NettyRemotingServer remotingServerMock = Mockito.mock(NettyRemotingServer.class);
        Mockito.when(brokerControllerMock.getRemotingServer()).thenReturn(remotingServerMock);
        Mockito.doNothing().when(remotingServerMock).processResponseCommand(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class));
        Mockito.when(telemetryCommandManager.getCommand(Mockito.eq(nonce))).thenReturn(new TelemetryCommandRecord(nonce, opaque));
        String jstack = "jstack";

        streamObserver.onNext(TelemetryCommand.newBuilder()
            .setThreadStackTrace(ThreadStackTrace.newBuilder()
                .setNonce(nonce)
                .setThreadStackTrace(jstack).build())
            .build());
        Mockito.verify(remotingServerMock, Mockito.times(1))
            .processResponseCommand(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class));
    }

    @Test
    public void testReportVerifyMessageResult() {
        int opaque = 1;
        String nonce = "123";
        NettyRemotingServer remotingServerMock = Mockito.mock(NettyRemotingServer.class);
        Mockito.when(brokerControllerMock.getRemotingServer()).thenReturn(remotingServerMock);
        Mockito.doNothing().when(remotingServerMock).processResponseCommand(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class));
        Mockito.when(telemetryCommandManager.getCommand(Mockito.eq(nonce))).thenReturn(new TelemetryCommandRecord(nonce, opaque));

        streamObserver.onNext(TelemetryCommand.newBuilder()
            .setVerifyMessageResult(VerifyMessageResult.newBuilder()
                .setNonce(nonce)
                .setStatus(ResponseBuilder.buildStatus(Code.OK, "ok")).build())
            .build());
        Mockito.verify(remotingServerMock, Mockito.times(1))
            .processResponseCommand(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class));
    }

    @Test
    public void testNotifyClientTermination() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);

        ClientManageProcessor clientManageProcessor = Mockito.mock(ClientManageProcessor.class);
        Mockito.when(brokerControllerMock.getClientManageProcessor()).thenReturn(clientManageProcessor);
        Mockito.when(clientManageProcessor.unregisterClient(Mockito.any(ChannelHandlerContext.class),
            Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        NotifyClientTerminationRequest request = NotifyClientTerminationRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .build();
        localGrpcService.notifyClientTermination(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        Mockito.verify(clientManageProcessor, Mockito.times(1))
            .unregisterClient(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class));
    }

    @Test
    public void testChangeInvisibleDuration() throws Exception {
        long invisibleTime = 1000L;
        int queueId = 1;
        long offset = 123L;
        ChangeInvisibleTimeResponseHeader responseHeader = new ChangeInvisibleTimeResponseHeader();
        responseHeader.setInvisibleTime(1000L);
        responseHeader.setPopTime(0L);
        responseHeader.setReviveQid(0);
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);

        ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor = Mockito.mock(ChangeInvisibleTimeProcessor.class);
        Mockito.when(brokerControllerMock.getChangeInvisibleTimeProcessor()).thenReturn(changeInvisibleTimeProcessor);
        Mockito.when(changeInvisibleTimeProcessor.processRequest(Mockito.any(ChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);
        ChangeInvisibleDurationRequest request = ChangeInvisibleDurationRequest.newBuilder().setReceiptHandle(
            ReceiptHandle.builder()
                .startOffset(0L)
                .retrieveTime(0L)
                .invisibleTime(invisibleTime)
                .nextVisibleTime(1000L)
                .reviveQueueId(0)
                .topicType("topic")
                .brokerName("brokerName")
                .queueId(queueId)
                .offset(offset)
                .build().encode()
        ).build();
        CompletableFuture<ChangeInvisibleDurationResponse> grpcFuture = localGrpcService.changeInvisibleDuration(
            Context.current()
                .withValue(InterceptorConstants.METADATA, metadata)
                .attach(), request);
        ChangeInvisibleDurationResponse r = grpcFuture.get();
        assertThat(r.getStatus().getCode()).isEqualTo(Code.OK);
        ReceiptHandle handle = ReceiptHandle.decode(r.getReceiptHandle());
        assertThat(handle.getInvisibleTime()).isEqualTo(invisibleTime);
        assertThat(handle.getQueueId()).isEqualTo(queueId);
        assertThat(handle.getOffset()).isEqualTo(offset);
    }
}