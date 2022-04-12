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

import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.ClientOverwrittenSettings;
import apache.rocketmq.v2.ClientSettings;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.DeadLetterPolicy;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import io.grpc.Channel;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingProcessor;
import org.apache.rocketmq.proxy.grpc.v2.service.ClusterGrpcService;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcForwardService;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID;
import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.awaitility.Awaitility.await;

public class ClusterGrpcTest extends GrpcBaseTest {

    private final int PORT = 8082;
    private GrpcForwardService grpcForwardService;
    private MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;
    private MessagingServiceGrpc.MessagingServiceStub stub;

    @Before
    public void setUp() throws Exception {
        super.setUp();
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
        grpcForwardService = new ClusterGrpcService();
        grpcForwardService.start();
        GrpcMessagingProcessor processor = new GrpcMessagingProcessor(grpcForwardService);
        setUpServer(processor, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);
        blockingStub = createBlockingStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
        stub = createStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));

        System.out.println(nsAddr);
        await().atMost(Duration.ofSeconds(40)).until(() -> {
            Map<String, BrokerData> brokerDataMap = MQAdminTestUtils.getCluster(nsAddr).getBrokerAddrTable();
            return brokerDataMap.size() == brokerNum;
        });
        System.out.println(MQAdminTestUtils.getCluster(nsAddr));
    }

    @After
    public void tearDown() throws Exception {
        grpcForwardService.shutdown();
        shutdown();
    }

    @Test
    public void testQueryRoute() throws Exception {
        String topic = initTopic();
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<ClientOverwrittenSettings> future = this.sendClientSettings(stub, ClientSettings.newBuilder()
            .setNonce(requestId)
            .setAccessPoint(Endpoints.newBuilder()
                .setScheme(AddressScheme.IPv4)
                .addAddresses(Address.newBuilder()
                    .setHost("127.0.0.1")
                    .setPort(PORT)
                    .build())
                .build())
            .build());
//        System.out.println(future.get());

//        TimeUnit.SECONDS.sleep(3);
        QueryRouteResponse response = blockingStub.queryRoute(buildQueryRouteRequest(topic));
        assertQueryRoute(response, brokerControllerList.size());
    }

    @Test
    public void testSendReceiveMessage() throws Exception {
        String topic = initTopicOnSampleTopicBroker(broker1Name);
        this.sendClientSettings(stub, ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PRODUCER)
            .build())
            .get();

        String group = "group";
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(topic, messageId));
        assertSendMessage(sendResponse, messageId);

        this.sendClientSettings(stub, ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSettings(Settings.newBuilder()
                .setSubscription(Subscription.newBuilder()
                    .setFifo(false)
                    .build())
                .build())
            .build())
            .get();

        ReceiveMessageResponse receiveResponse = blockingStub.withDeadlineAfter(3, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(group, topic));
        assertReceiveMessage(receiveResponse, messageId);
        String receiptHandle = receiveResponse.getMessages(0).getSystemProperties().getReceiptHandle();
        AckMessageResponse ackMessageResponse = blockingStub.ackMessage(buildAckMessageRequest(group, topic, receiptHandle));
        assertAck(ackMessageResponse);
    }

    @Test
    public void testSendReceiveMessageThenToDLQ() throws Exception {
        String topic = initTopicOnSampleTopicBroker(broker1Name);
        this.sendClientSettings(stub, ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PRODUCER)
            .build())
            .get();

        String group = "group";
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(topic, messageId));
        assertSendMessage(sendResponse, messageId);

        this.sendClientSettings(stub, ClientSettings.newBuilder()
            .setNonce(UUID.randomUUID().toString())
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSettings(Settings.newBuilder()
                .setSubscription(Subscription.newBuilder()
                    .setDeadLetterPolicy(DeadLetterPolicy.newBuilder()
                        .setMaxDeliveryAttempts(2)
                        .build())
                    .setFifo(false)
                    .build())
                .build())
            .build())
            .get();

        ReceiveMessageResponse receiveResponse = blockingStub.withDeadlineAfter(20, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(group, topic));
        assertReceiveMessage(receiveResponse, messageId);

        Message message = receiveResponse.getMessages(0);
        NackMessageResponse nackMessageResponse = blockingStub.nackMessage(buildNackMessageRequest(
            group, topic, messageId, message.getSystemProperties().getReceiptHandle(), 1
        ));
        assertNackMessageResponse(nackMessageResponse);

        AtomicReference<ReceiveMessageResponse> receiveRetryResponseRef = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            ReceiveMessageResponse receiveRetryResponse = blockingStub.withDeadlineAfter(20, TimeUnit.SECONDS)
                .receiveMessage(buildReceiveMessageRequest(group, topic));
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
        MessageQueue dlqMQ = new MessageQueue(MixAll.getDLQTopic(group), topic, 0);
        await().atMost(Duration.ofSeconds(10)).until(() -> {
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

        System.out.println(1);
    }


}
