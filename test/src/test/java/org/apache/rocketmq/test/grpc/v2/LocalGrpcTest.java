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
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingProcessor;
import org.apache.rocketmq.proxy.grpc.v2.service.LocalGrpcService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalGrpcTest extends GrpcBaseTest {
    private LocalGrpcService localGrpcService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        localGrpcService = new LocalGrpcService(brokerController1);
        localGrpcService.start();
        GrpcMessagingProcessor processor = new GrpcMessagingProcessor(localGrpcService);
        setUpServer(processor, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);
    }

    @After
    public void clean() throws Exception {
        localGrpcService.shutdown();
        shutdown();
    }

    @Test
    public void testQueryRoute() throws Exception {
        String topic = initTopic();
        this.sendClientSettings(stub, buildAccessPointClientSettings(PORT)).get();

        QueryRouteResponse response = blockingStub.queryRoute(buildQueryRouteRequest(topic));
        assertQueryRoute(response, brokerControllerList.size() * defaultQueueNums);
    }

    @Test
    public void testSendReceiveMessage() throws Exception {
        super.testSendReceiveMessage();
    }

    @Test
    public void testTransactionCheckThenCommit() {
        super.testTransactionCheckThenCommit();
    }

    @Test
    public void testSendReceiveMessageThenToDLQ() throws Exception {
        super.testSendReceiveMessageThenToDLQ();
    }

    @Test
    public void testSimpleConsumerSendAndRecv() throws Exception {
        super.testSimpleConsumerSendAndRecv();
    }

    @Test
    public void testSimpleConsumerToDLQ() throws Exception {
        super.testSimpleConsumerToDLQ();
    }
}
