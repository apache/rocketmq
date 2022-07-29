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
package org.apache.rocketmq.broker.processor;

import com.google.common.base.Preconditions;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public class CommonMergeBatchResponseStrategy extends MergeBatchResponseStrategy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private CommonMergeBatchResponseStrategy() {
    }

    private static final CommonMergeBatchResponseStrategy INSTANCE = new CommonMergeBatchResponseStrategy();

    @Override
    public CompletableFuture<RemotingCommand> mergeResponses(
            RemotingCommand batchRequest,
            Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture) {
        Preconditions.checkNotNull(batchRequest, "batchRequest shouldn't be null.");
        Preconditions.checkNotNull(opaqueToFuture, "opaqueToFuture shouldn't be null.");
        Preconditions.checkArgument(!opaqueToFuture.isEmpty());

        final int batchOpaque = batchRequest.getOpaque();

        CompletableFuture<RemotingCommand> batchFuture = new CompletableFuture<>();
        int expectedResponseNum = opaqueToFuture.size();

        ConcurrentHashMap<Integer, RemotingCommand> responses = new ConcurrentHashMap<>(expectedResponseNum);

        opaqueToFuture.forEach((childOpaque, childFuture) -> childFuture.whenComplete((childResp, throwable) -> {
            if (throwable != null) {
                log.error("Something is wrong with pull-merging. batch: {}, child: {}.", batchOpaque, childOpaque, throwable);
                responses.put(childOpaque, RemotingCommand.createResponse(childOpaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
            } else {
                responses.put(childOpaque, nonNullableResponse(childOpaque, childResp));
            }
            completeBatchFuture(batchFuture, responses, expectedResponseNum, batchOpaque, childOpaque, batchRequest);
        }));

        return batchFuture;
    }

    public static CommonMergeBatchResponseStrategy getInstance() {
        return INSTANCE;
    }

}
