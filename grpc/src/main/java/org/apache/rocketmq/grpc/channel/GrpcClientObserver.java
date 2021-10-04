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

package org.apache.rocketmq.grpc.channel;

import apache.rocketmq.v1.GenericPollingResponse;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import com.google.rpc.Code;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.grpc.common.InterceptorConstants;
import org.apache.rocketmq.grpc.common.ResponseBuilder;
import org.apache.rocketmq.grpc.common.ResponseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcClientObserver {

    public static Logger log = LoggerFactory.getLogger(GrpcClientObserver.class);

    public final static int TIMEOUT_MILLIS = 30 * 1000;

    private static final ScheduledExecutorService DEADLINE_CHECKER =
        new ScheduledThreadPoolExecutor(3, new ThreadFactoryImpl("GRPC_DEADLINE_CHECKER_"));

    private StreamObserver<MultiplexingResponse> clientObserver;

    private CompletableFuture<MultiplexingRequest> nextRequestFuture;

    private final String group;

    private final String clientId;

    private String sequence;

    private long lastUseTimestamp;

    public GrpcClientObserver(String group, String clientId) {
        this.group = group;
        this.clientId = clientId;
    }

    public void setClientObserver(MultiplexingRequest request,
        StreamObserver<MultiplexingResponse> clientStreamObserver) {
        this.lastUseTimestamp = System.currentTimeMillis();
        this.sequence = UUID.randomUUID().toString();
        this.clientObserver = clientStreamObserver;
        log.info("grpc client observer set. sequence: {}, group: {}, clientId: {}, type: {}, clientHost: {}",
            sequence, group, clientId, request.getTypeCase(),
            InterceptorConstants.METADATA.get(io.grpc.Context.current()).get(InterceptorConstants.REMOTE_ADDRESS));

        DEADLINE_CHECKER.schedule(() -> {
            log.info("grpc client observer exceed deadline. sequence: {}, group: {}, clientId: {}",
                sequence, group, clientId);

            GenericPollingResponse genericPollingResponse = GenericPollingResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.OK, "server deadline exceeded"))
                .build();

            MultiplexingResponse multiplexingResponse = MultiplexingResponse.newBuilder()
                .setPollingResponse(genericPollingResponse)
                .build();

            ResponseWriter.write(clientStreamObserver, multiplexingResponse);
        }, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<MultiplexingRequest> call(MultiplexingResponse response) {
        this.lastUseTimestamp = System.currentTimeMillis();

        log.info("grpc client call. sequence: {}, group: {}, clientId: {}, type: {}",
            sequence, group, clientId, response.getTypeCase());

        this.nextRequestFuture = new CompletableFuture<>();
        ResponseWriter.write(this.clientObserver, response);
        return this.nextRequestFuture;
    }

    public void result(MultiplexingRequest request, StreamObserver<MultiplexingResponse> newClientObserver) {
        log.info("grpc client result. sequence: {}, group: {}, clientId: {}, type: {}",
            sequence, group, clientId, request.getTypeCase());

        this.nextRequestFuture.complete(request);
        this.setClientObserver(request, newClientObserver);
    }

    public long getLastUseTimestamp() {
        return lastUseTimestamp;
    }
}
