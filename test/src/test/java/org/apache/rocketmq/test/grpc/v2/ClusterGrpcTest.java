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

import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteResponse;
import java.time.Duration;
import java.util.Map;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingProcessor;
import org.apache.rocketmq.proxy.grpc.v2.service.ClusterGrpcService;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcForwardService;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class ClusterGrpcTest extends GrpcBaseTest {

    private GrpcForwardService grpcForwardService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ConfigurationManager.getProxyConfig().setTransactionHeartbeatPeriodSecond(3);
        grpcForwardService = new ClusterGrpcService();
        grpcForwardService.start();
        GrpcMessagingProcessor processor = new GrpcMessagingProcessor(grpcForwardService);
        setUpServer(processor, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);

        await().atMost(Duration.ofSeconds(40)).until(() -> {
            Map<String, BrokerData> brokerDataMap = MQAdminTestUtils.getCluster(nsAddr).getBrokerAddrTable();
            return brokerDataMap.size() == brokerNum;
        });
    }

    @After
    public void tearDown() throws Exception {
        grpcForwardService.shutdown();
        shutdown();
    }

    @Test
    public void testQueryRoute() throws Exception {
        String topic = initTopic();

        QueryRouteResponse response = blockingStub.queryRoute(buildQueryRouteRequest(topic));
        assertQueryRoute(response, brokerNum * defaultQueueNums);
    }

    @Test
    public void testQueryAssignment() throws Exception {
        String topic = initTopic();
        String group = "group";

        QueryAssignmentResponse response = blockingStub.queryAssignment(buildQueryAssignmentRequest(topic, group));

        assertQueryAssignment(response, brokerNum);
    }

    @Test
    public void testTransactionCheckThenCommit() {
        super.testTransactionCheckThenCommit();
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
