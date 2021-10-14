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

import apache.rocketmq.v1.NoopCommand;
import apache.rocketmq.v1.PollCommandResponse;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.grpc.common.InterceptorConstants;
import org.apache.rocketmq.grpc.common.ResponseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcClientObserver {

    public static Logger log = LoggerFactory.getLogger(GrpcClientObserver.class);

    public final static int TIMEOUT_MILLIS = 30 * 1000;

    private static final ScheduledExecutorService DEADLINE_CHECKER =
        new ScheduledThreadPoolExecutor(3, new ThreadFactoryImpl("GRPC_DEADLINE_CHECKER_"));

    private StreamObserver<PollCommandResponse> clientObserver;

    private CompletableFuture<GeneratedMessageV3> resultFuture;

    private final String group;

    private final String clientId;

    private String sequence;

    private long lastUseTimestamp;

    public GrpcClientObserver(String group, String clientId) {
        this.group = group;
        this.clientId = clientId;
    }

    public void setClientObserver(StreamObserver<PollCommandResponse> clientStreamObserver) {
        this.lastUseTimestamp = System.currentTimeMillis();
        this.sequence = UUID.randomUUID().toString();
        this.clientObserver = clientStreamObserver;
        log.info("grpc client observer set. sequence: {}, group: {}, clientId: {}, clientHost: {}",
            sequence, group, clientId, InterceptorConstants.METADATA.get(io.grpc.Context.current()).get(InterceptorConstants.REMOTE_ADDRESS));

        DEADLINE_CHECKER.schedule(() -> {
            log.info("grpc client observer exceed deadline. sequence: {}, group: {}, clientId: {}",
                sequence, group, clientId);

            NoopCommand noopCommand = NoopCommand.newBuilder().build();

            PollCommandResponse multiplexingResponse = PollCommandResponse.newBuilder()
                .setNoopCommand(noopCommand)
                .build();

            ResponseWriter.write(clientStreamObserver, multiplexingResponse);
        }, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<GeneratedMessageV3> call(PollCommandResponse response) {
        this.lastUseTimestamp = System.currentTimeMillis();

        log.info("grpc client call. sequence: {}, group: {}, clientId: {}, type: {}",
            sequence, group, clientId, response.getTypeCase());

        this.resultFuture = new CompletableFuture<>();
        ResponseWriter.write(this.clientObserver, response);
        return this.resultFuture;
    }

    /**
     * Save the result after call client
     */
    public void setResult(GeneratedMessageV3 request) {
        log.info("grpc client result. sequence: {}, group: {}, clientId: {}, requestName: {}",
            sequence, group, clientId, request.getDescriptorForType().getFullName());

        this.resultFuture.complete(request);
    }

    public long getLastUseTimestamp() {
        return lastUseTimestamp;
    }
}
