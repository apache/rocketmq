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

package org.apache.rocketmq.test.grpc.v2;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.ClientOverwrittenSettings;
import apache.rocketmq.v2.ClientSettings;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DeadLetterPolicy;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.PullMessageRequest;
import apache.rocketmq.v2.PullMessageResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryOffsetPolicy;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SystemProperties;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.TransactionResolution;
import apache.rocketmq.v2.TransactionSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.net.URL;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.test.base.BaseConf;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID;
import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class GrpcBaseTest extends BaseConf {

    protected final int PORT = 8082;
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    protected MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;
    protected MessagingServiceGrpc.MessagingServiceStub stub;
    protected final Metadata header = new Metadata();

    protected static final int defaultQueueNums = 8;

    public void setUp() throws Exception {
        header.put(InterceptorConstants.CLIENT_ID, "client-id" + UUID.randomUUID());
        header.put(InterceptorConstants.LANGUAGE, "JAVA");

        String mockProxyHome = "/mock/rmq/proxy/home";
        URL mockProxyHomeURL = getClass().getClassLoader().getResource("rmq-proxy-home");
        if (mockProxyHomeURL != null) {
            mockProxyHome = mockProxyHomeURL.toURI().getPath();
        }
        System.setProperty(RMQ_PROXY_HOME, mockProxyHome);
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        ConfigurationManager.getProxyConfig().setGrpcServerPort(PORT);
        ConfigurationManager.getProxyConfig().setNameSrvAddr(nsAddr);

        blockingStub = createBlockingStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
        stub = createStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
    }

    protected MessagingServiceGrpc.MessagingServiceStub createStub(Channel channel) {
        MessagingServiceGrpc.MessagingServiceStub stub = MessagingServiceGrpc.newStub(channel);
        return MetadataUtils.attachHeaders(stub, header);
    }

    protected MessagingServiceGrpc.MessagingServiceBlockingStub createBlockingStub(Channel channel) {
        MessagingServiceGrpc.MessagingServiceBlockingStub stub = MessagingServiceGrpc.newBlockingStub(channel);
        return MetadataUtils.attachHeaders(stub, header);
    }

    protected CompletableFuture<ClientOverwrittenSettings> sendClientSettings(MessagingServiceGrpc.MessagingServiceStub stub, ClientSettings clientSettings) {
        CompletableFuture<ClientOverwrittenSettings> future = new CompletableFuture<>();
        StreamObserver<TelemetryCommand> requestStreamObserver = stub.telemetry(new DefaultTelemetryCommandStreamObserver() {
            @Override
            public void onNext(TelemetryCommand value) {
                TelemetryCommand.CommandCase commandCase = value.getCommandCase();
                if (TelemetryCommand.CommandCase.CLIENT_OVERWRITTEN_SETTINGS.equals(commandCase)) {
                    future.complete(value.getClientOverwrittenSettings());
                }
            }
        });
        requestStreamObserver.onNext(TelemetryCommand.newBuilder()
            .setClientSettings(clientSettings)
            .build());
        requestStreamObserver.onCompleted();
        return future;
    }

    protected void setUpServer(MessagingServiceGrpc.MessagingServiceImplBase serverImpl,
        int port, boolean enableInterceptor) throws IOException, CertificateException {
        SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
        ServerServiceDefinition serviceDefinition = ServerInterceptors.intercept(serverImpl);
        if (enableInterceptor) {
            serviceDefinition = ServerInterceptors.intercept(serverImpl, new ContextInterceptor(), new HeaderInterceptor());
        }
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(NettyServerBuilder.forPort(port)
            .directExecutor()
            .addService(serviceDefinition)
            .useTransportSecurity(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
            .build()
            .start());
    }

    protected Channel createChannel(int port) throws SSLException {
        return grpcCleanup.register(NettyChannelBuilder.forAddress("127.0.0.1", port)
            .directExecutor()
            .sslContext(SslContextBuilder
                .forClient()
                .sslProvider(SslProvider.OPENSSL)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocolConfig(new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    ApplicationProtocolNames.HTTP_2))
                .build()
            )
            .build());
    }

    public void testSendReceiveMessage() throws Exception {
        String topic = initTopicOnSampleTopicBroker(broker1Name);
        String group = "group";

        this.sendClientSettings(stub, ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PRODUCER)
            .build())
            .get();

        // init consumer offset
        receiveMessage(blockingStub, topic, group);

        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(topic, messageId));
        assertSendMessage(sendResponse, messageId);

        this.sendClientSettings(stub, buildPushConsumerClientSettings()).get();

        ReceiveMessageResponse response = receiveMessage(blockingStub, topic, group);
        assertReceiveMessage(response, messageId);
        String receiptHandle = response.getMessages(0).getSystemProperties().getReceiptHandle();
        AckMessageResponse ackMessageResponse = blockingStub.ackMessage(buildAckMessageRequest(group, topic, receiptHandle));
        assertAck(ackMessageResponse);
    }

    public void testSendReceiveMessageThenToDLQ() throws Exception {
        String topic = initTopicOnSampleTopicBroker(broker1Name);
        this.sendClientSettings(stub, ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PRODUCER)
            .build())
            .get();

        String group = "group";

        // init consumer offset
        receiveMessage(blockingStub, topic, group);

        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(topic, messageId));
        assertSendMessage(sendResponse, messageId);

        this.sendClientSettings(stub, buildPushConsumerClientSettings()).get();

        ReceiveMessageResponse receiveResponse = receiveMessage(blockingStub, topic, group);
        assertReceiveMessage(receiveResponse, messageId);

        Message message = receiveResponse.getMessages(0);
        NackMessageResponse nackMessageResponse = blockingStub.nackMessage(buildNackMessageRequest(
            group, topic, messageId, message.getSystemProperties().getReceiptHandle(), 1
        ));
        assertNackMessageResponse(nackMessageResponse);

        AtomicReference<ReceiveMessageResponse> receiveRetryResponseRef = new AtomicReference<>();
        await().atMost(java.time.Duration.ofSeconds(30)).until(() -> {
            ReceiveMessageResponse receiveRetryResponse = receiveMessage(blockingStub, topic, group, 1);
            if (receiveRetryResponse.getMessagesCount() <= 0) {
                return false;
            }
            receiveRetryResponseRef.set(receiveRetryResponse);
            return receiveRetryResponse.getMessages(0).getSystemProperties()
                .getMessageId().equals(messageId);
        });

        message = receiveRetryResponseRef.get().getMessages(0);
        nackMessageResponse = blockingStub.nackMessage(buildNackMessageRequest(
            group, topic, messageId, message.getSystemProperties().getReceiptHandle(), 2
        ));
        assertNackMessageResponse(nackMessageResponse);

        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(group);
        defaultMQPullConsumer.start();
        org.apache.rocketmq.common.message.MessageQueue dlqMQ = new org.apache.rocketmq.common.message.MessageQueue(MixAll.getDLQTopic(group), broker1Name, 0);
        await().atMost(java.time.Duration.ofSeconds(10)).until(() -> {
            try {
                PullResult pullResult = defaultMQPullConsumer.pull(dlqMQ, "*", 0L, 1);
                if (!PullStatus.FOUND.equals(pullResult.getPullStatus())) {
                    return false;
                }
                MessageExt messageExt = pullResult.getMsgFoundList().get(0);
                return messageId.equals(messageExt.getMsgId());
            } catch (Throwable ignore) {
                return false;
            }
        });
    }

    @Test
    public void testTransactionCheckThenCommit() {
        String topic = initTopicOnSampleTopicBroker(broker1Name);
        String group = "group";

        AtomicReference<TelemetryCommand> telemetryCommandRef = new AtomicReference<>(null);
        StreamObserver<TelemetryCommand> requestStreamObserver = stub.telemetry(new DefaultTelemetryCommandStreamObserver() {
            @Override
            public void onNext(TelemetryCommand value) {
                telemetryCommandRef.set(value);
            }
        });

        try {
            requestStreamObserver.onNext(TelemetryCommand.newBuilder()
                .setClientSettings(buildPushConsumerClientSettings())
                .build());
            await().atMost(java.time.Duration.ofSeconds(3)).until(() -> {
                if (telemetryCommandRef.get() == null) {
                    return false;
                }
                if (telemetryCommandRef.get().getCommandCase() != TelemetryCommand.CommandCase.CLIENT_OVERWRITTEN_SETTINGS) {
                    return false;
                }
                return telemetryCommandRef.get() != null;
            });
            telemetryCommandRef.set(null);
            // init consumer offset
            receiveMessage(blockingStub, topic, group);

            requestStreamObserver.onNext(TelemetryCommand.newBuilder()
                .setClientSettings(buildProducerClientSettings(topic))
                .build());
            blockingStub.heartbeat(HeartbeatRequest.newBuilder()
                .setGroup(Resource.newBuilder()
                    .setName(group)
                    .build())
                .build());
            await().atMost(java.time.Duration.ofSeconds(3)).until(() -> {
                if (telemetryCommandRef.get() == null) {
                    return false;
                }
                if (telemetryCommandRef.get().getCommandCase() != TelemetryCommand.CommandCase.CLIENT_OVERWRITTEN_SETTINGS) {
                    return false;
                }
                return telemetryCommandRef.get() != null;
            });
            telemetryCommandRef.set(null);

            String messageId = createUniqID();
            SendMessageResponse sendResponse = blockingStub.sendMessage(buildTransactionSendMessageRequest(topic, messageId));
            assertSendMessage(sendResponse, messageId);

            await().atMost(java.time.Duration.ofSeconds(60)).until(() -> {
                if (telemetryCommandRef.get() == null) {
                    return false;
                }
                if (telemetryCommandRef.get().getCommandCase() != TelemetryCommand.CommandCase.RECOVER_ORPHANED_TRANSACTION_COMMAND) {
                    return false;
                }
                return telemetryCommandRef.get() != null;
            });
            RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand = telemetryCommandRef.get().getRecoverOrphanedTransactionCommand();
            assertRecoverOrphanedTransactionCommand(recoverOrphanedTransactionCommand, messageId);

            EndTransactionResponse endTransactionResponse = blockingStub.endTransaction(
                buildEndTransactionRequest(topic, messageId, recoverOrphanedTransactionCommand.getTransactionId(), TransactionResolution.COMMIT));
            assertEndTransactionResponse(endTransactionResponse);

            requestStreamObserver.onNext(TelemetryCommand.newBuilder()
                .setClientSettings(buildPushConsumerClientSettings())
                .build());

            await().atMost(java.time.Duration.ofSeconds(30)).until(() -> {
                ReceiveMessageResponse receiveRetryResponse = receiveMessage(blockingStub, topic, group);
                if (receiveRetryResponse.getMessagesCount() <= 0) {
                    return false;
                }
                return receiveRetryResponse.getMessages(0).getSystemProperties()
                    .getMessageId().equals(messageId);
            });
        } finally {
            requestStreamObserver.onCompleted();
        }
    }

    public void testPullMessage() throws Exception {
        String topic = initTopicOnSampleTopicBroker(broker1Name);
        String group = "group";
        String messageId = createUniqID();

        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        assertSendMessage(blockingStub.sendMessage(buildSendMessageRequest(topic, messageId)), messageId);

        this.sendClientSettings(stub, buildPullConsumerClientSettings()).get();

        QueryOffsetResponse queryOffsetResponse = blockingStub.queryOffset(buildQueryOffsetRequest(broker1Name, topic, QueryOffsetPolicy.BEGINNING));
        assertQueryOffsetResponse(queryOffsetResponse, 0L);

        queryOffsetResponse = blockingStub.queryOffset(buildQueryOffsetRequest(broker1Name, topic, QueryOffsetPolicy.END));
        assertQueryOffsetResponse(queryOffsetResponse, 1L);

        await().atMost(java.time.Duration.ofSeconds(10)).until(() -> {
            PullMessageResponse response = blockingStub.withDeadlineAfter(20, TimeUnit.SECONDS)
                .pullMessage(buildPullMessageRequest(broker1Name, group, topic, 0L));
            if (response.getMessagesCount() <= 0) {
                return false;
            }
            return response.getMessages(0).getSystemProperties().getMessageId().equals(messageId);
        });
    }

    public ReceiveMessageResponse receiveMessage(MessagingServiceGrpc.MessagingServiceBlockingStub stub, String topic, String group) {
        return stub.withDeadlineAfter(15, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(group, topic));
    }

    public ReceiveMessageResponse receiveMessage(MessagingServiceGrpc.MessagingServiceBlockingStub stub, String topic, String group, int timeSeconds) {
        return stub.withDeadlineAfter(timeSeconds, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(group, topic));
    }

    public QueryRouteRequest buildQueryRouteRequest(String topic) {
        return QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName(topic)
                .build())
            .build();
    }

    public QueryAssignmentRequest buildQueryAssignmentRequest(String topic, String group) {
        return QueryAssignmentRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(topic).build())
            .setGroup(Resource.newBuilder().setName(group).build())
            .build();
    }

    public SendMessageRequest buildSendMessageRequest(String topic, String messageId) {
        return SendMessageRequest.newBuilder()
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .build())
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(MessageType.NORMAL)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .setBody(ByteString.copyFromUtf8("123"))
                .build())
            .build();
    }

    public SendMessageRequest buildTransactionSendMessageRequest(String topic, String messageId) {
        return SendMessageRequest.newBuilder()
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .build())
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(MessageType.TRANSACTION)
                    .setOrphanedTransactionRecoveryDuration(Duration.newBuilder().setSeconds(10))
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(RemotingUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .setBody(ByteString.copyFromUtf8("123"))
                .build())
            .build();
    }

    public ReceiveMessageRequest buildReceiveMessageRequest(String group, String topic) {
        return ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName(group)
                .build())
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setId(0)
                .build())
            .setBatchSize(16)
            .setInvisibleDuration(Duration.newBuilder()
                .setSeconds(3)
                .build())
            .build();
    }

    public AckMessageRequest buildAckMessageRequest(String group, String topic, String receiptHandle) {
        return AckMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName(group)
                .build())
            .setTopic(Resource.newBuilder()
                .setName(topic)
                .build())
            .setReceiptHandle(receiptHandle)
            .build();
    }

    public NackMessageRequest buildNackMessageRequest(String group, String topic, String messageId, String receiptHandle,
        int deliveryAttempt) {
        return NackMessageRequest.newBuilder()
            .setDeliveryAttempt(deliveryAttempt)
            .setMessageId(messageId)
            .setReceiptHandle(receiptHandle)
            .setTopic(Resource.newBuilder()
                .setName(topic)
                .build())
            .setGroup(Resource.newBuilder()
                .setName(group)
                .build())
            .build();
    }

    public EndTransactionRequest buildEndTransactionRequest(String topic, String messageId, String transactionId, TransactionResolution resolution) {
        return EndTransactionRequest.newBuilder()
            .setMessageId(messageId)
            .setTopic(Resource.newBuilder()
                .setName(topic)
                .build())
            .setTransactionId(transactionId)
            .setResolution(resolution)
            .setSource(TransactionSource.SOURCE_SERVER_CHECK)
            .build();
    }

    public QueryOffsetRequest buildQueryOffsetRequest(String brokerName, String topic, QueryOffsetPolicy queryOffsetPolicy) {
        return QueryOffsetRequest.newBuilder()
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder().setName(topic).build())
                .setBroker(Broker.newBuilder().setName(brokerName).build())
                .setId(0)
                .build())
            .setPolicy(queryOffsetPolicy)
            .build();
    }

    public PullMessageRequest buildPullMessageRequest(String brokerName, String group, String topic, long offset) {
        return PullMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(group).build())
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder().setName(topic).build())
                .setBroker(Broker.newBuilder().setName(brokerName).build())
                .setId(0)
                .build())
            .setBatchSize(32)
            .setOffset(offset)
            .build();
    }

    public void assertQueryRoute(QueryRouteResponse response, int messageQueueSize) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(response.getMessageQueuesList().size()).isEqualTo(messageQueueSize);
        assertThat(response.getMessageQueues(0).getBroker().getEndpoints().getAddresses(0).getPort()).isEqualTo(ConfigurationManager.getProxyConfig().getGrpcServerPort());
    }

    public void assertQueryAssignment(QueryAssignmentResponse response, int assignmentCount) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(response.getAssignmentsCount()).isEqualTo(assignmentCount);
        assertThat(response.getAssignments(0).getMessageQueue().getBroker().getEndpoints().getAddresses(0).getPort()).isEqualTo(ConfigurationManager.getProxyConfig().getGrpcServerPort());
    }

    public void assertSendMessage(SendMessageResponse response, String messageId) {
        assertThat(response.getStatus()
            .getCode()).isEqualTo(Code.OK);
        assertThat(response.getReceipts(0).getMessageId()).isEqualTo(messageId);
    }

    public void assertReceiveMessage(ReceiveMessageResponse response, String messageId) {
        assertThat(response.getStatus()
            .getCode()).isEqualTo(Code.OK);
        assertThat(response.getMessagesCount()).isEqualTo(1);
        assertThat(response.getMessages(0)
            .getSystemProperties()
            .getMessageId()).isEqualTo(messageId);
    }

    public void assertAck(AckMessageResponse response) {
        assertThat(response.getStatus()
            .getCode()).isEqualTo(Code.OK);
    }

    public void assertNackMessageResponse(NackMessageResponse response) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
    }

    public void assertRecoverOrphanedTransactionCommand(RecoverOrphanedTransactionCommand command, String messageId) {
        assertThat(command.getOrphanedTransactionalMessage().getSystemProperties().getMessageId())
            .isEqualTo(messageId);
        assertThat(command.getTransactionId()).isNotBlank();
    }

    public void assertEndTransactionResponse(EndTransactionResponse response) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
    }

    public void assertQueryOffsetResponse(QueryOffsetResponse response, long offset) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(response.getOffset()).isEqualTo(offset);
    }

    public ClientSettings buildAccessPointClientSettings(int port) {
        return ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setAccessPoint(Endpoints.newBuilder()
                .setScheme(AddressScheme.IPv4)
                .addAddresses(Address.newBuilder()
                    .setHost("127.0.0.1")
                    .setPort(port)
                    .build())
                .build())
            .build();
    }

    public ClientSettings buildPushConsumerClientSettings() {
        return buildPushConsumerClientSettings(2, false);
    }

    public ClientSettings buildPushConsumerClientSettings(int maxDeliveryAttempts, boolean fifo) {
        return ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSettings(Settings.newBuilder()
                .setSubscription(Subscription.newBuilder()
                    .setDeadLetterPolicy(DeadLetterPolicy.newBuilder()
                        .setMaxDeliveryAttempts(maxDeliveryAttempts)
                        .build())
                    .setFifo(fifo)
                    .build())
                .build())
            .build();
    }

    public ClientSettings buildPullConsumerClientSettings() {
        return ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PULL_CONSUMER)
            .setSettings(Settings.newBuilder()
                .build())
            .build();
    }

    public ClientSettings buildSimpleConsumerClientSettings() {
        return ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .setSettings(Settings.newBuilder()
                .build())
            .build();
    }

    public ClientSettings buildProducerClientSettings(String... topics) {
        List<Resource> topicResources = Arrays.stream(topics).map(topic -> Resource.newBuilder().setName(topic).build())
            .collect(Collectors.toList());
        return ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PRODUCER)
            .setSettings(Settings.newBuilder()
                .setPublishing(Publishing.newBuilder()
                    .addAllTopics(topicResources)
                    .build())
                .build())
            .build();
    }

    protected static class DefaultTelemetryCommandStreamObserver implements StreamObserver<TelemetryCommand> {

        @Override
        public void onNext(TelemetryCommand value) {

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }
}