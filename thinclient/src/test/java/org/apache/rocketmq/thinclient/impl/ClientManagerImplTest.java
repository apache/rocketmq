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

package org.apache.rocketmq.thinclient.impl;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.SendMessageRequest;
import io.grpc.Metadata;
import java.time.Duration;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.junit.Test;

public class ClientManagerImplTest extends TestBase {
    private final ClientManagerImpl clientManager = new ClientManagerImpl();

    @Test
    public void testQueryRoute() {
        Metadata metadata = new Metadata();
        QueryRouteRequest request = QueryRouteRequest.newBuilder().build();
        clientManager.queryRoute(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.queryRoute(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testHeartbeat() {
        Metadata metadata = new Metadata();
        HeartbeatRequest request = HeartbeatRequest.newBuilder().build();
        clientManager.heartbeat(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.heartbeat(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testSendMessage() {
        Metadata metadata = new Metadata();
        SendMessageRequest request = SendMessageRequest.newBuilder().build();
        clientManager.sendMessage(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.sendMessage(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testQueryAssignment() {
        Metadata metadata = new Metadata();
        QueryAssignmentRequest request = QueryAssignmentRequest.newBuilder().build();
        clientManager.queryAssignment(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.queryAssignment(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testReceiveMessage() {
        Metadata metadata = new Metadata();
        ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder().build();
        clientManager.receiveMessage(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.receiveMessage(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testAckMessage() {
        Metadata metadata = new Metadata();
        AckMessageRequest request = AckMessageRequest.newBuilder().build();
        clientManager.ackMessage(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.ackMessage(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testChangeInvisibleDuration() {
        Metadata metadata = new Metadata();
        ChangeInvisibleDurationRequest request = ChangeInvisibleDurationRequest.newBuilder().build();
        clientManager.changeInvisibleDuration(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.changeInvisibleDuration(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() {
        Metadata metadata = new Metadata();
        ForwardMessageToDeadLetterQueueRequest request = ForwardMessageToDeadLetterQueueRequest.newBuilder().build();
        clientManager.forwardMessageToDeadLetterQueue(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.forwardMessageToDeadLetterQueue(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testEndTransaction() {
        Metadata metadata = new Metadata();
        EndTransactionRequest request = EndTransactionRequest.newBuilder().build();
        clientManager.endTransaction(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.endTransaction(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }

    @Test
    public void testNotifyClientTermination() {
        Metadata metadata = new Metadata();
        NotifyClientTerminationRequest request = NotifyClientTerminationRequest.newBuilder().build();
        clientManager.notifyClientTermination(fakeEndpoints(), metadata, request, Duration.ofSeconds(1));
        clientManager.notifyClientTermination(null, metadata, request, Duration.ofSeconds(1));
        // Expect no exception thrown.
    }
}