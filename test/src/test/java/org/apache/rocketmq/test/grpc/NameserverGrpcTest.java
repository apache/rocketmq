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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.grpc;

import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.Resource;
import com.google.rpc.Code;
import io.grpc.Channel;
import java.io.IOException;
import java.security.cert.CertificateException;
import org.apache.rocketmq.namesrv.grpc.NameServerGrpcService;
import org.apache.rocketmq.test.base.GrpcBaseTest;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NameserverGrpcTest extends GrpcBaseTest {
    private MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;

    protected final NameServerGrpcService nameServerService = new NameServerGrpcService(namesrvController);

    @Before
    public void setUp() throws IOException, CertificateException {
        int grpcPort = 9896;
        int remotingPort = namesrvController.getNettyServerConfig()
            .getListenPort();
        if (grpcPort == remotingPort) {
            grpcPort += 1;
        }
        namesrvController.getNettyServerConfig()
            .setHttp2ProxyPort(grpcPort);
        Channel channel = setUpServer(nameServerService, grpcPort, remotingPort, false);
        blockingStub = MessagingServiceGrpc.newBlockingStub(channel);
    }

    @Test
    public void testQueryRoute() {
        QueryRouteRequest request = QueryRouteRequest.newBuilder()
            .setTopic(apache.rocketmq.v1.Resource.newBuilder()
                .setName(clusterName)
                .build())
            .build();
        QueryRouteResponse response = blockingStub.queryRoute(request);
        assertThat(response.getCommon()
            .getStatus()
            .getCode()).isEqualTo(Code.OK.getNumber());
        assertThat(response.getPartitionsList()
            .size()).isEqualTo(32);
        for (Partition partition : response.getPartitionsList()) {
            assertThat(partition.getTopic()).isEqualTo(Resource.newBuilder()
                .setName(clusterName)
                .build());
        }
    }
}
