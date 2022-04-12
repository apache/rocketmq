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
import apache.rocketmq.v2.ClientOverwrittenSettings;
import apache.rocketmq.v2.ClientSettings;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SystemProperties;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
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
import java.security.cert.CertificateException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.test.base.BaseConf;
import org.junit.Rule;

import static org.assertj.core.api.Assertions.assertThat;

public class GrpcBaseTest extends BaseConf {
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    protected final Metadata header = new Metadata();

    private static final int defaultQueueNums = 8;

    public void setUp() throws Exception {
        header.put(InterceptorConstants.CLIENT_ID, "client-id" + UUID.randomUUID());
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
        CompletableFuture<ClientOverwrittenSettings> future = CompletableFuture.completedFuture(ClientOverwrittenSettings.getDefaultInstance());
        StreamObserver<TelemetryCommand> requestStreamObserver = stub.telemetry(new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                TelemetryCommand.CommandCase commandCase = value.getCommandCase();
                if (TelemetryCommand.CommandCase.CLIENT_OVERWRITTEN_SETTINGS.equals(commandCase)) {
                    future.complete(value.getClientOverwrittenSettings());
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        });
        requestStreamObserver.onNext(TelemetryCommand.newBuilder()
            .setClientSettings(clientSettings)
            .build());
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

    public QueryRouteRequest buildQueryRouteRequest(String topic) {
        return QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName(topic)
                .build())
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
            .setInitializationTimestamp(Timestamp.newBuilder()
                .setSeconds(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
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

    public void assertQueryRoute(QueryRouteResponse response, int brokerSize) {
        assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(response.getMessageQueuesList().size()).isEqualTo(brokerSize * defaultQueueNums);
        assertThat(response.getMessageQueues(0).getBroker().getEndpoints().getAddresses(0).getPort()).isEqualTo(ConfigurationManager.getProxyConfig().getGrpcServerPort());
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
}