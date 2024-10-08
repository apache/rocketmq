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

import apache.rocketmq.v2.QueryRouteResponse;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runners.MethodSorters;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class LocalGrpcIT extends GrpcBaseIT {

    private MessagingProcessor messagingProcessor;
    private GrpcMessagingApplication grpcMessagingApplication;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController1);
        messagingProcessor.start();
        grpcMessagingApplication = GrpcMessagingApplication.create(messagingProcessor);
        grpcMessagingApplication.start();
        setUpServer(grpcMessagingApplication, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);
    }

    @After
    public void clean() throws Exception {
        messagingProcessor.shutdown();
        grpcMessagingApplication.shutdown();
        shutdown();
    }

    @Test
    public void testQueryRoute() throws Exception {
        String topic = initTopic();

        QueryRouteResponse response = blockingStub.queryRoute(buildQueryRouteRequest(topic));
        assertQueryRoute(response, brokerControllerList.size() * DEFAULT_QUEUE_NUMS);
    }

    @Test
    public void testQueryAssignment() throws Exception {
        super.testQueryAssignment();
    }

    @Test
    public void testQueryFifoAssignment() throws Exception {
        super.testQueryFifoAssignment();
    }

    @Test
    public void testTransactionCheckThenCommit() {
        super.testTransactionCheckThenCommit();
    }

    @Test
    @Ignore
    public void testSimpleConsumerSendAndRecvDelayMessage() throws Exception {
        super.testSimpleConsumerSendAndRecvDelayMessage();
    }

    @Test
    public void testSimpleConsumerSendAndRecvBigMessage() throws Exception {
        super.testSimpleConsumerSendAndRecvBigMessage();
    }

    @Test
    public void testSimpleConsumerSendAndRecv() throws Exception {
        super.testSimpleConsumerSendAndRecv();
    }

    @Test
    public void testSimpleConsumerToDLQ() throws Exception {
        super.testSimpleConsumerToDLQ();
    }

    @Test
    public void testConsumeOrderly() throws Exception {
        super.testConsumeOrderly();
    }
}
