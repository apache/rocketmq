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

package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.Resource;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RouteServiceTest {
    public static final String BROKER_NAME = "brokerName";
    public static final String NAMESPACE = "namespace";
    public static final String TOPIC = "topic";
    public static final Broker MOCK_BROKER = Broker.newBuilder().setName(BROKER_NAME).build();
    public static final Resource MOCK_TOPIC = Resource.newBuilder()
            .setName(TOPIC)
            .setResourceNamespace(NAMESPACE)
            .build();

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }


    @Test
    public void testGenPartitionFromQueueData() throws Exception {
        // test queueData with 8 read queues, 8 write queues, and rw permission, expect 8 rw queues.
        QueueData queueDataWith8R8WPermRW = mockQueueData(8, 8, PermName.PERM_READ | PermName.PERM_WRITE);
        List<Partition> partitionWith8R8WPermRW =  RouteService.genPartitionFromQueueData(queueDataWith8R8WPermRW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R8WPermRW.size()).isEqualTo(8);
        assertThat(partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(8);
        assertThat(partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(0);
        assertThat(partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(0);

        // test queueData with 8 read queues, 8 write queues, and read only permission, expect 8 read only queues.
        QueueData queueDataWith8R8WPermR = mockQueueData(8, 8, PermName.PERM_READ);
        List<Partition> partitionWith8R8WPermR =  RouteService.genPartitionFromQueueData(queueDataWith8R8WPermR, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R8WPermR.size()).isEqualTo(8);
        assertThat(partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(8);
        assertThat(partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(0);
        assertThat(partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(0);

        // test queueData with 8 read queues, 8 write queues, and write only permission, expect 8 write only queues.
        QueueData queueDataWith8R8WPermW = mockQueueData(8, 8, PermName.PERM_WRITE);
        List<Partition> partitionWith8R8WPermW =  RouteService.genPartitionFromQueueData(queueDataWith8R8WPermW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R8WPermW.size()).isEqualTo(8);
        assertThat(partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(8);
        assertThat(partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(0);
        assertThat(partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(0);

        // test queueData with 8 read queues, 0 write queues, and rw permission, expect 8 read only queues.
        QueueData queueDataWith8R0WPermRW = mockQueueData(8, 0, PermName.PERM_READ | PermName.PERM_WRITE);
        List<Partition> partitionWith8R0WPermRW =  RouteService.genPartitionFromQueueData(queueDataWith8R0WPermRW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R0WPermRW.size()).isEqualTo(8);
        assertThat(partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(8);
        assertThat(partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(0);
        assertThat(partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(0);

        // test queueData with 4 read queues, 8 write queues, and rw permission, expect 4 rw queues and  4 write only queues.
        QueueData queueDataWith4R8WPermRW = mockQueueData(4, 8, PermName.PERM_READ | PermName.PERM_WRITE);
        List<Partition> partitionWith4R8WPermRW =  RouteService.genPartitionFromQueueData(queueDataWith4R8WPermRW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith4R8WPermRW.size()).isEqualTo(8);
        assertThat(partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(4);
        assertThat(partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(4);
        assertThat(partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(0);

    }

    private QueueData mockQueueData(int r, int w, int perm) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(BROKER_NAME);
        queueData.setReadQueueNums(r);
        queueData.setWriteQueueNums(w);
        queueData.setPerm(perm);
        return queueData;
    }

} 
