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
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageResponse;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingProcessor;
import org.apache.rocketmq.proxy.grpc.v2.service.LocalGrpcService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID;
import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;

public class LocalGrpcTest extends GrpcBaseTest {
    private MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;
    private MessagingServiceGrpc.MessagingServiceStub stub;
    private LocalGrpcService localGrpcService;

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
        ConfigurationManager.getProxyConfig().setGrpcServerPort(8082);
        ConfigurationManager.getProxyConfig().setNameSrvAddr(nsAddr);
        localGrpcService = new LocalGrpcService(brokerController1);
        localGrpcService.start();
        GrpcMessagingProcessor processor = new GrpcMessagingProcessor(localGrpcService);
        setUpServer(processor, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);
        blockingStub = createBlockingStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
        stub = createStub(createChannel(ConfigurationManager.getProxyConfig().getGrpcServerPort()));
    }

    @After
    public void clean() throws Exception {
        localGrpcService.shutdown();
        shutdown();
    }

    @Test
    public void testQueryRoute() {
        String topic = initTopic();
        QueryRouteResponse response = blockingStub.queryRoute(buildQueryRouteRequest(topic));
        assertQueryRoute(response, brokerControllerList.size());
    }

    @Test
    public void testSendReceiveMessage() {
        String group = "group";
        String messageId = createUniqID();
        SendMessageResponse sendResponse = blockingStub.sendMessage(buildSendMessageRequest(broker1Name, messageId));
        assertSendMessage(sendResponse, messageId);

        ReceiveMessageResponse receiveResponse = blockingStub.withDeadlineAfter(3, TimeUnit.SECONDS)
            .receiveMessage(buildReceiveMessageRequest(group, broker1Name));
        assertReceiveMessage(receiveResponse, messageId);
        String receiptHandle = receiveResponse.getMessages(0).getSystemProperties().getReceiptHandle();
        AckMessageResponse ackMessageResponse = blockingStub.ackMessage(buildAckMessageRequest(group, broker1Name, receiptHandle));
        assertAck(ackMessageResponse);
    }
}
