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

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.RecallMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.RetryPolicy;
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
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.Server;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.Rule;

import static org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID;
import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class GrpcBaseIT extends BaseConf {

    /**
     * Let OS pick up an available port.
     */
    private int port = 0;

    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    protected MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;
    protected MessagingServiceGrpc.MessagingServiceStub stub;
    protected final Metadata header = new Metadata();

    protected static final int DEFAULT_QUEUE_NUMS = 8;

    public void setUp() throws Exception {
        brokerController1.getBrokerConfig().setTransactionCheckInterval(3 * 1000);
        brokerController2.getBrokerConfig().setTransactionCheckInterval(3 * 1000);
        brokerController3.getBrokerConfig().setTransactionCheckInterval(3 * 1000);

        header.put(GrpcConstants.CLIENT_ID, "client-id" + UUID.randomUUID());
        header.put(GrpcConstants.LANGUAGE, "JAVA");

        String mockProxyHome = "/mock/rmq/proxy/home";
        URL mockProxyHomeURL = getClass().getClassLoader().getResource("rmq-proxy-home");
        if (mockProxyHomeURL != null) {
            mockProxyHome = mockProxyHomeURL.toURI().getPath();
        }

        if (null != mockProxyHome) {
            System.setProperty(RMQ_PROXY_HOME, mockProxyHome);
        }

        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        ConfigurationManager.getProxyConfig().setNamesrvAddr(NAMESRV_ADDR);
        // Set LongPollingReserveTimeInMillis to 500ms to reserve more time for IT
        ConfigurationManager.getProxyConfig().setLongPollingReserveTimeInMillis(500);
        ConfigurationManager.getProxyConfig().setRocketMQClusterName(brokerController1.getBrokerConfig().getBrokerClusterName());
        ConfigurationManager.getProxyConfig().setHeartbeatSyncerTopicClusterName(brokerController1.getBrokerConfig().getBrokerClusterName());
        ConfigurationManager.getProxyConfig().setMinInvisibleTimeMillsForRecv(3);
        ConfigurationManager.getProxyConfig().setGrpcClientConsumerMinLongPollingTimeoutMillis(0);
    }

    protected MessagingServiceGrpc.MessagingServiceStub createStub(Channel channel) {
        MessagingServiceGrpc.MessagingServiceStub stub = MessagingServiceGrpc.newStub(channel);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(header));
    }

    protected MessagingServiceGrpc.MessagingServiceBlockingStub createBlockingStub(Channel channel) {
        MessagingServiceGrpc.MessagingServiceBlockingStub stub = MessagingServiceGrpc.newBlockingStub(channel);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(header));
    }

    protected CompletableFuture<Settings> sendClientSettings(MessagingServiceGrpc.MessagingServiceStub stub,
        Settings clientSettings) {
        CompletableFuture<Settings> future = new CompletableFuture<>();
        StreamObserver<TelemetryCommand> requestStreamObserver = stub.telemetry(new DefaultTelemetryCommandStreamObserver() {
            @Override
            public void onNext(TelemetryCommand value) {
                TelemetryCommand.CommandCase commandCase = value.getCommandCase();
                if (TelemetryCommand.CommandCase.SETTINGS.equals(commandCase)) {
                    future.complete(value.getSettings());
                }
            }
        });
        requestStreamObserver.onNext(TelemetryCommand.newBuilder()
            .setSettings(clientSettings)
            .build());
        future.whenComplete((settings, throwable) -> requestStreamObserver.onCompleted());
        return future;
    }

    protected void setUpServer(MessagingServiceGrpc.MessagingServiceImplBase serverImpl,
        int port, boolean enableInterceptor) throws IOException, CertificateException {
        SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
        ServerServiceDefinition serviceDefinition = ServerInterceptors.intercept(serverImpl);
        if (enableInterceptor) {
            serviceDefinition = ServerInterceptors.intercept(serverImpl, new ContextInterceptor(), new HeaderInterceptor());
        }
        Server server = NettyServerBuilder.forPort(port)
            .directExecutor()
            .addService(serviceDefinition)
            .useTransportSecurity(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
            .build()
            .start();
        this.port = server.getPort();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(server);

        ConfigurationManager.getProxyConfig().setGrpcServerPort(this.port);
        blockingStub = createBlockingStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
        stub = createStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
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

    public void testQueryAssignment() throws Exception {
        String topic = initTopic();
        String group = "group";

        QueryAssignmentResponse response = blockingStub.queryAssignment(buildQueryAssignmentRequest(topic, group));

        assertQueryAssignment(response, BROKER_NUM);
    }

    public void testQueryFifoAssignment() throws Exception {
        String topic = initTopic(TopicMessageType.FIFO);
        String group = MQRandomUtils.getRandomConsumerGroup();
        SubscriptionGroupConfig groupConfig = brokerController1.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        groupConfig.setConsumeMessageOrderly(true);
        brokerController1.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);
        brokerController2.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);
        brokerController3.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);

        QueryAssignmentResponse response = blockingStub.queryAssignment(buildQueryAssignmentRequest(topic, group));

        assertQueryAssignment(response, BROKER_NUM * QUEUE_NUMBERS);
    }

    public void testTransactionCheckThenCommit() {
        String topic = initTopicOnSampleTopicBroker(BROKER1_NAME, TopicMessageType.TRANSACTION);
        String group = MQRandomUtils.getRandomConsumerGroup();

        AtomicReference<TelemetryCommand> telemetryCommandRef = new AtomicReference<>(null);
        StreamObserver<TelemetryCommand> requestStreamObserver = stub.telemetry(new DefaultTelemetryCommandStreamObserver() {
            @Override
            public void onNext(TelemetryCommand value) {
                telemetryCommandRef.set(value);
            }
        });

        try {
            requestStreamObserver.onNext(TelemetryCommand.newBuilder()
                .setSettings(buildPushConsumerClientSettings(group))
                .build());
            await().atMost(java.time.Duration.ofSeconds(3)).until(() -> {
                if (telemetryCommandRef.get() == null) {
                    return false;
                }
                if (telemetryCommandRef.get().getCommandCase() != TelemetryCommand.CommandCase.SETTINGS) {
                    return false;
                }
                return telemetryCommandRef.get() != null;
            });
            telemetryCommandRef.set(null);
            // init consumer offset
            receiveMessage(blockingStub, topic, group, 1);

            requestStreamObserver.onNext(TelemetryCommand.newBuilder()
                .setSettings(buildProducerClientSettings(topic))
                .build());
            blockingStub.heartbeat(buildHeartbeatRequest(group));
            await().atMost(java.time.Duration.ofSeconds(3)).until(() -> {
                if (telemetryCommandRef.get() == null) {
                    blockingStub.heartbeat(buildHeartbeatRequest(group));
                    return false;
                }
                if (telemetryCommandRef.get().getCommandCase() != TelemetryCommand.CommandCase.SETTINGS) {
                    blockingStub.heartbeat(buildHeartbeatRequest(group));
                    return false;
                }
                return telemetryCommandRef.get() != null;
            });
            telemetryCommandRef.set(null);

            String messageId = createUniqID();
            SendMessageResponse sendResponse = blockingStub.sendMessage(buildTransactionSendMessageRequest(topic, messageId));
            assertSendMessage(sendResponse, messageId);

            await().atMost(java.time.Duration.ofMinutes(2)).until(() -> {
                if (telemetryCommandRef.get() == null) {
                    blockingStub.heartbeat(buildHeartbeatRequest(group));
                    return false;
                }
                if (telemetryCommandRef.get().getCommandCase() != TelemetryCommand.CommandCase.RECOVER_ORPHANED_TRANSACTION_COMMAND) {
                    blockingStub.heartbeat(buildHeartbeatRequest(group));
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
                .setSettings(buildPushConsumerClientSettings(group))
                .build());

            await().atMost(java.time.Duration.ofSeconds(30)).until(() -> {
                List<Message> retryMessageList = getMessageFromReceiveMessageResponse(receiveMessage(blockingStub, topic, group));
                if (retryMessageList.isEmpty()) {
                    return false;
                }
                return retryMessageList.get(0).getSystemProperties()
                    .getMessageId().equals(messageId);
            });
        } finally {
            requestStreamObserver.onCompleted();
        }
    }

    public HeartbeatRequest buildHeartbeatRequest(String group) {
        return HeartbeatRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName(group)
                .build())
            .build();
    }

    public void testSimpleConsumerSendAndRecvDelayMessage() throws Exception {
        String topic = initTopicOnSampleTopicBroker(BROKER1_NAME, TopicMessageType.DELAY);
        String group = MQRandomUtils.getRandomConsumerGroup();
        long delayTime = TimeUnit.SECONDS.toMillis(5);

        // init consumer offset
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(blockingStub, topic, group, 1);

        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(MessageType.NORMAL)
                    .setBodyEncoding(Encoding.GZIP)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis() + delayTime))
                    .build())
                .setBody(ByteString.copyFromUtf8("hello"))
                .build())
            .build());
        long sendTime = System.currentTimeMillis();
        assertSendMessage(sendResponse, messageId);

        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();

        AtomicLong recvTime = new AtomicLong();
        AtomicReference<Message> recvMessage = new AtomicReference<>();
        await().atMost(java.time.Duration.ofSeconds(10)).until(() -> {
            List<Message> messageList = getMessageFromReceiveMessageResponse(receiveMessage(blockingStub, topic, group));
            if (messageList.isEmpty()) {
                return false;
            }
            recvTime.set(System.currentTimeMillis());
            recvMessage.set(messageList.get(0));
            return messageList.get(0).getSystemProperties().getMessageId().equals(messageId);
        });

        assertThat(Math.abs(recvTime.get() - sendTime - delayTime) < 2 * 1000).isTrue();
    }

    public void testSimpleConsumerSendAndRecallDelayMessage() throws Exception {
        String topic = initTopicOnSampleTopicBroker(BROKER1_NAME, TopicMessageType.DELAY);
        String group = MQRandomUtils.getRandomConsumerGroup();
        long delayTime = TimeUnit.SECONDS.toMillis(5);

        // init consumer offset
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(blockingStub, topic, group, 1);

        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(MessageType.DELAY)
                    .setBodyEncoding(Encoding.GZIP)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .setDeliveryTimestamp(Timestamps.fromMillis(System.currentTimeMillis() + delayTime))
                    .build())
                .setBody(ByteString.copyFromUtf8("hello"))
                .build())
            .build());
        long sendTime = System.currentTimeMillis();
        assertSendMessage(sendResponse, messageId);
        String recallHandle = sendResponse.getEntries(0).getRecallHandle();
        assertThat(recallHandle).isNotEmpty();

        RecallMessageRequest recallRequest = RecallMessageRequest.newBuilder()
            .setRecallHandle(recallHandle)
            .setTopic(Resource.newBuilder().setResourceNamespace("").setName(topic).build())
            .build();
        RecallMessageResponse recallResponse =
            blockingStub.withDeadlineAfter(2, TimeUnit.SECONDS).recallMessage(recallRequest);
        assertThat(recallResponse.getStatus()).isEqualTo(
            ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()));
        assertThat(recallResponse.getMessageId()).isEqualTo(messageId);

        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();

        AtomicLong recvTime = new AtomicLong();
        AtomicReference<Message> recvMessage = new AtomicReference<>();
        try {
            await().atMost(java.time.Duration.ofSeconds(10)).until(() -> {
                List<Message> messageList = getMessageFromReceiveMessageResponse(receiveMessage(blockingStub, topic, group));
                if (messageList.isEmpty()) {
                    return false;
                }
                recvTime.set(System.currentTimeMillis());
                recvMessage.set(messageList.get(0));
                return messageList.get(0).getSystemProperties().getMessageId().equals(messageId);
            });
        } catch (Exception e) {
        }
        assertThat(recvTime.get()).isEqualTo(0L);
        assertThat(recvMessage.get()).isNull();
    }

    public void testSimpleConsumerSendAndRecvBigMessage() throws Exception {
        String topic = initTopicOnSampleTopicBroker(BROKER1_NAME);
        String group = MQRandomUtils.getRandomConsumerGroup();

        int bodySize = 4 * 1024;

        // init consumer offset
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(blockingStub, topic, group, 1);

        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendBigMessageRequest(topic, messageId, bodySize));
        assertSendMessage(sendResponse, messageId);

        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();

        Message message = assertAndGetReceiveMessage(receiveMessage(blockingStub, topic, group), messageId);
        assertThat(message.getSystemProperties().getBodyEncoding()).isEqualTo(Encoding.GZIP);
        assertThat(message.getBody().size()).isEqualTo(bodySize);
    }

    public void testSimpleConsumerSendAndRecv() throws Exception {
        String topic = initTopicOnSampleTopicBroker(BROKER1_NAME);
        String group = MQRandomUtils.getRandomConsumerGroup();

        // init consumer offset
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(blockingStub, topic, group, 1);

        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(topic, messageId));
        assertSendMessage(sendResponse, messageId);
        assertThat(sendResponse.getEntries(0).getRecallHandle()).isNullOrEmpty();

        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();

        Message message = assertAndGetReceiveMessage(receiveMessage(blockingStub, topic, group), messageId);

        String receiptHandle = message.getSystemProperties().getReceiptHandle();
        ChangeInvisibleDurationResponse changeResponse = blockingStub.changeInvisibleDuration(buildChangeInvisibleDurationRequest(topic, group, receiptHandle, 5));
        assertChangeInvisibleDurationResponse(changeResponse, receiptHandle);

        List<String> ackHandles = new ArrayList<>();
        ackHandles.add(changeResponse.getReceiptHandle());

        await().atMost(java.time.Duration.ofSeconds(20)).until(() -> {
            List<Message> retryMessageList = getMessageFromReceiveMessageResponse(receiveMessage(blockingStub, topic, group));
            if (retryMessageList.isEmpty()) {
                return false;
            }
            if (retryMessageList.get(0).getSystemProperties()
                .getMessageId().equals(messageId)) {
                ackHandles.add(retryMessageList.get(0).getSystemProperties().getReceiptHandle());
                return true;
            }
            return false;
        });

        assertThat(ackHandles.size()).isEqualTo(2);
        AckMessageResponse ackMessageResponse = blockingStub.ackMessage(buildAckMessageRequest(topic, group,
            AckMessageEntry.newBuilder().setMessageId(messageId).setReceiptHandle(ackHandles.get(0)).build(),
            AckMessageEntry.newBuilder().setMessageId(messageId).setReceiptHandle(ackHandles.get(1)).build()));
        assertThat(ackMessageResponse.getStatus().getCode()).isEqualTo(Code.MULTIPLE_RESULTS);
        int okNum = 0;
        int expireNum = 0;
        for (AckMessageResultEntry entry : ackMessageResponse.getEntriesList()) {
            if (entry.getStatus().getCode().equals(Code.OK)) {
                okNum++;
            } else if (entry.getStatus().getCode().equals(Code.INVALID_RECEIPT_HANDLE)) {
                expireNum++;
            }
        }
        assertThat(okNum).isEqualTo(1);
        assertThat(expireNum).isEqualTo(1);
    }

    public void testSimpleConsumerToDLQ() throws Exception {
        String topic = initTopicOnSampleTopicBroker(BROKER1_NAME);
        String group = MQRandomUtils.getRandomConsumerGroup();
        int maxDeliveryAttempts = 2;

        SubscriptionGroupConfig groupConfig = brokerController1.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        groupConfig.setRetryMaxTimes(maxDeliveryAttempts - 1);
        brokerController1.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);
        brokerController2.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);
        brokerController3.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);

        // init consumer offset
        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();
        receiveMessage(blockingStub, topic, group, 1);

        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(topic, messageId));
        assertSendMessage(sendResponse, messageId);

        this.sendClientSettings(stub, buildSimpleConsumerClientSettings(group)).get();

        AtomicInteger receiveMessageCount = new AtomicInteger(0);

        assertAndGetReceiveMessage(receiveMessage(blockingStub, topic, group), messageId);
        receiveMessageCount.incrementAndGet();

        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(group);
        defaultMQPullConsumer.start();
        org.apache.rocketmq.common.message.MessageQueue dlqMQ = new org.apache.rocketmq.common.message.MessageQueue(MixAll.getDLQTopic(group), BROKER1_NAME, 0);
        await().atMost(java.time.Duration.ofSeconds(30)).until(() -> {
            try {
                List<Message> messageList = getMessageFromReceiveMessageResponse(receiveMessage(blockingStub, topic, group, 1));
                receiveMessageCount.addAndGet(messageList.size());

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

        assertThat(receiveMessageCount.get()).isEqualTo(maxDeliveryAttempts);
    }

    public void testConsumeOrderly() throws Exception {
        String topic = initTopicOnSampleTopicBroker(BROKER1_NAME, TopicMessageType.FIFO);
        String group = MQRandomUtils.getRandomConsumerGroup();

        SubscriptionGroupConfig groupConfig = brokerController1.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        groupConfig.setConsumeMessageOrderly(true);
        brokerController1.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);
        brokerController2.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);
        brokerController3.getSubscriptionGroupManager().updateSubscriptionGroupConfig(groupConfig);

        this.sendClientSettings(stub, buildPushConsumerClientSettings(group)).get();
        receiveMessage(blockingStub, topic, group, 1);

        String messageGroup = "group";
        this.sendClientSettings(stub, buildProducerClientSettings(topic)).get();
        List<String> messageIdList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String messageId = createUniqID();
            messageIdList.add(messageId);
            SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendOrderMessageRequest(topic, messageId, messageGroup));
            assertSendMessage(sendResponse, messageId);
        }

        List<String> messageRecvList = new ArrayList<>();
        this.sendClientSettings(stub, buildPushConsumerClientSettings(group)).get();
        await().atMost(java.time.Duration.ofSeconds(20)).until(() -> {
            List<Message> retryMessageList = getMessageFromReceiveMessageResponse(receiveMessage(blockingStub, topic, group));
            if (retryMessageList.isEmpty()) {
                return false;
            }
            for (Message message : retryMessageList) {
                String messageId = message.getSystemProperties().getMessageId();
                messageRecvList.add(messageId);
                blockingStub.ackMessage(buildAckMessageRequest(topic, group,
                    AckMessageEntry.newBuilder().setMessageId(messageId).setReceiptHandle(message.getSystemProperties().getReceiptHandle()).build()));
            }
            return messageRecvList.size() == messageIdList.size();
        });

        for (int i = 0; i < messageIdList.size(); i++) {
            assertThat(messageRecvList.get(i)).isEqualTo(messageIdList.get(i));
        }
    }

    public List<ReceiveMessageResponse> receiveMessage(MessagingServiceGrpc.MessagingServiceBlockingStub stub,
        String topic, String group) {
        return receiveMessage(stub, topic, group, 15);
    }

    public List<ReceiveMessageResponse> receiveMessage(MessagingServiceGrpc.MessagingServiceBlockingStub stub,
        String topic, String group, int timeSeconds) {
        List<ReceiveMessageResponse> responseList = new ArrayList<>();
        Iterator<ReceiveMessageResponse> responseIterator = stub.withDeadlineAfter(timeSeconds, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(topic, group));
        while (responseIterator.hasNext()) {
            responseList.add(responseIterator.next());
        }
        return responseList;
    }

    public List<Message> getMessageFromReceiveMessageResponse(List<ReceiveMessageResponse> responseList) {
        List<Message> messageList = new ArrayList<>();
        for (ReceiveMessageResponse response : responseList) {
            if (response.hasMessage()) {
                messageList.add(response.getMessage());
            }
        }
        return messageList;
    }

    public QueryRouteRequest buildQueryRouteRequest(String topic) {
        return QueryRouteRequest.newBuilder()
            .setEndpoints(buildEndpoints(port))
            .setTopic(Resource.newBuilder()
                .setName(topic)
                .build())
            .build();
    }

    public QueryAssignmentRequest buildQueryAssignmentRequest(String topic, String group) {
        return QueryAssignmentRequest.newBuilder()
            .setEndpoints(buildEndpoints(port))
            .setTopic(Resource.newBuilder().setName(topic).build())
            .setGroup(Resource.newBuilder().setName(group).build())
            .build();
    }

    public SendMessageRequest buildSendMessageRequest(String topic, String messageId) {
        return SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(MessageType.NORMAL)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .setBody(ByteString.copyFromUtf8("123"))
                .build())
            .build();
    }

    public SendMessageRequest buildSendOrderMessageRequest(String topic, String messageId, String messageGroup) {
        return SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(MessageType.FIFO)
                    .setMessageGroup(messageGroup)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .setBody(ByteString.copyFromUtf8("123"))
                .build())
            .build();
    }

    public SendMessageRequest buildSendBigMessageRequest(String topic, String messageId, int messageSize) {
        return SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId(messageId)
                    .setQueueId(0)
                    .setMessageType(MessageType.NORMAL)
                    .setBodyEncoding(Encoding.GZIP)
                    .setBornTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .setBody(ByteString.copyFromUtf8(RandomUtils.getStringWithCharacter(messageSize)))
                .build())
            .build();
    }

    public SendMessageRequest buildTransactionSendMessageRequest(String topic, String messageId) {
        return SendMessageRequest.newBuilder()
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
                    .setBornHost(StringUtils.defaultString(NetworkUtil.getLocalAddress(), "127.0.0.1:1234"))
                    .build())
                .setBody(ByteString.copyFromUtf8("123"))
                .build())
            .build();
    }

    public ReceiveMessageRequest buildReceiveMessageRequest(String topic, String group) {
        return ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName(group)
                .build())
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic)
                    .build())
                .setId(-1)
                .build())
            .setBatchSize(1)
            .setAutoRenew(false)
            .setInvisibleDuration(Duration.newBuilder()
                .setSeconds(3)
                .build())
            .build();
    }

    public AckMessageRequest buildAckMessageRequest(String topic, String group, AckMessageEntry... entry) {
        return AckMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName(group)
                .build())
            .setTopic(Resource.newBuilder()
                .setName(topic)
                .build())
            .addAllEntries(Arrays.stream(entry).collect(Collectors.toList()))
            .build();
    }

    public EndTransactionRequest buildEndTransactionRequest(String topic, String messageId, String transactionId,
        TransactionResolution resolution) {
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

    public ChangeInvisibleDurationRequest buildChangeInvisibleDurationRequest(String topic, String group,
        String receiptHandle, int second) {
        return ChangeInvisibleDurationRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(topic).build())
            .setGroup(Resource.newBuilder().setName(group).build())
            .setInvisibleDuration(Durations.fromSeconds(second))
            .setReceiptHandle(receiptHandle)
            .build();
    }

    public void assertQueryRoute(QueryRouteResponse response, int messageQueueSize) {
        assertThat(response.getStatus()).isEqualTo(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()));
        assertThat(response.getMessageQueuesList().size()).isEqualTo(messageQueueSize);
        assertThat(response.getMessageQueues(0).getBroker().getEndpoints().getAddresses(0).getPort()).isEqualTo(ConfigurationManager.getProxyConfig().getGrpcServerPort());
    }

    public void assertQueryAssignment(QueryAssignmentResponse response, int assignmentCount) {
        assertThat(response.getStatus()).isEqualTo(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()));
        assertThat(response.getAssignmentsCount()).isEqualTo(assignmentCount);
        assertThat(response.getAssignments(0).getMessageQueue().getBroker().getEndpoints().getAddresses(0).getPort()).isEqualTo(ConfigurationManager.getProxyConfig().getGrpcServerPort());
    }

    public void assertSendMessage(SendMessageResponse response, String messageId) {
        assertThat(response.getStatus()).isEqualTo(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()));
        assertThat(response.getEntries(0).getMessageId()).isEqualTo(messageId);
    }

    public Message assertAndGetReceiveMessage(List<ReceiveMessageResponse> response, String messageId) {
        assertThat(response.get(0).hasStatus()).isTrue();
        assertThat(response.get(0).getStatus()
            .getCode()).isEqualTo(Code.OK);
        assertThat(response.get(1).getMessage()
            .getSystemProperties()
            .getMessageId()).isEqualTo(messageId);
        return response.get(1).getMessage();
    }

    public void assertRecoverOrphanedTransactionCommand(RecoverOrphanedTransactionCommand command, String messageId) {
        assertThat(command.getTransactionId()).isNotBlank();
    }

    public void assertEndTransactionResponse(EndTransactionResponse response) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
    }

    public void assertChangeInvisibleDurationResponse(ChangeInvisibleDurationResponse response, String prevHandle) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(response.getReceiptHandle()).isNotEqualTo(prevHandle);
    }

    public Endpoints buildEndpoints(int port) {
        return Endpoints.newBuilder()
            .setScheme(AddressScheme.IPv4)
            .addAddresses(Address.newBuilder()
                .setHost("127.0.0.1")
                .setPort(port)
                .build())
            .build();
    }

    public Settings buildSimpleConsumerClientSettings(String group) {
        return Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .setRequestTimeout(Durations.fromSeconds(3))
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(group).build())
                .build())
            .build();
    }

    public Settings buildPushConsumerClientSettings(String group) {
        return buildPushConsumerClientSettings(2, group);
    }

    public Settings buildPushConsumerClientSettings(int maxDeliveryAttempts, String group) {
        return Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setRequestTimeout(Durations.fromSeconds(3))
            .setBackoffPolicy(RetryPolicy.newBuilder()
                .setMaxAttempts(maxDeliveryAttempts)
                .build())
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(group).build())
                .build())
            .build();
    }

    public Settings buildProducerClientSettings(String... topics) {
        List<Resource> topicResources = Arrays.stream(topics).map(topic -> Resource.newBuilder().setName(topic).build())
            .collect(Collectors.toList());
        return Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addAllTopics(topicResources)
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
