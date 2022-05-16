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
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalGrpcIT extends GrpcBaseIT {

    private ServiceManager serviceManager;
    private GrpcMessagingApplication grpcMessagingApplication;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        serviceManager = ServiceManager.createForClusterMode();
        serviceManager.start();
        grpcMessagingApplication = GrpcMessagingApplication.create(serviceManager);
        grpcMessagingApplication.start();
        setUpServer(grpcMessagingApplication, ConfigurationManager.getProxyConfig().getGrpcServerPort(), true);
    }

    @After
    public void clean() throws Exception {
        serviceManager.shutdown();
        grpcMessagingApplication.shutdown();
        shutdown();
    }

    @Test
    public void testQueryRoute() throws Exception {
        String topic = initTopic();

        QueryRouteResponse response = blockingStub.queryRoute(buildQueryRouteRequest(topic));
        assertQueryRoute(response, brokerControllerList.size() * defaultQueueNums);
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
