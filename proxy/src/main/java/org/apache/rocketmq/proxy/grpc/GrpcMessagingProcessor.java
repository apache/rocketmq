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

package org.apache.rocketmq.proxy.grpc;

import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.grpc.common.ResponseWriter;
import org.apache.rocketmq.proxy.grpc.service.GrpcService;

public class GrpcMessagingProcessor extends MessagingServiceGrpc.MessagingServiceImplBase {
    private final GrpcService grpcService;

    public GrpcMessagingProcessor(GrpcService grpcService) {
        this.grpcService = grpcService;
    }

    @Override
    public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        CompletableFuture<SendMessageResponse> future = grpcService.sendMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }
}
