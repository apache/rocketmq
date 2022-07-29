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

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.CommonBatchRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.rocketmq.broker.processor.MergeBatchResponseStrategy.REMARK_SYSTEM_ERROR;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class PullMessageCommonMergeStrategyTest {
    private PullMessageCommonMergeStrategy strategy;
    private Random random = new Random();

    @Before
    public void init() {
        strategy = PullMessageCommonMergeStrategy.getInstance();
    }

    @Test
    public void testPartialDone() throws ExecutionException, InterruptedException, RemotingCommandException {
        CommonBatchRequestHeader header = new CommonBatchRequestHeader();
        RemotingCommand batchRequest = RemotingCommand.createRequestCommand(RequestCode.COMMON_BATCH_REQUEST, header);

        int totalResponseNum = 20;
        AtomicInteger increment = new AtomicInteger(0);

        Map<Integer, CompletableFuture<RemotingCommand>> allResults = new HashMap<>();

        Map<Integer, CompletableFuture<RemotingCommand>> doneResults = new HashMap<>();
        for (int i = 0; i < totalResponseNum; i++) {
            Integer opaque = increment.getAndIncrement();
            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            future.complete(RemotingCommand.createResponse(opaque, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
            doneResults.put(opaque, future);
        }

        Map<Integer, CompletableFuture<RemotingCommand>> undoneResults = new HashMap<>();
        for (int i = 0; i < totalResponseNum; i++) {
            Integer opaque = increment.getAndIncrement();
            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            undoneResults.put(opaque, future);
        }

        allResults.putAll(doneResults);
        allResults.putAll(undoneResults);

        CompletableFuture<RemotingCommand> batchFuture = strategy.mergeResponses(batchRequest, allResults);
        Assert.assertTrue(batchFuture.isDone());

        RemotingCommand actualBatchResponse = batchFuture.get();
        assertThat(actualBatchResponse.getOpaque()).isEqualTo(batchRequest.getOpaque());

        List<RemotingCommand> actualChildResponses = RemotingCommand.parseChildren(actualBatchResponse);
        Assert.assertEquals(totalResponseNum * 2, actualChildResponses.size());

        Map<Integer, RemotingCommand> actualChildResponseMap = actualChildResponses
                .stream()
                .collect(Collectors.toMap(RemotingCommand::getOpaque, x -> x));

        for (Map.Entry<Integer, CompletableFuture<RemotingCommand>> entry : doneResults.entrySet()) {
            Integer opaque = entry.getKey();
            CompletableFuture<RemotingCommand> expectedResponseFuture = entry.getValue();
            RemotingCommand expectedResponse = expectedResponseFuture.get();
            RemotingCommand actualResponse = actualChildResponseMap.get(opaque);
            Assert.assertEquals(expectedResponse.getCode(), actualResponse.getCode());
            Assert.assertEquals(expectedResponse.getBody(), actualResponse.getBody());
            Assert.assertEquals(expectedResponse.getCode(), actualResponse.getCode());
        }
    }

    @Test
    public void testAllUndone() throws ExecutionException, InterruptedException, RemotingCommandException {
        CommonBatchRequestHeader header = new CommonBatchRequestHeader();
        RemotingCommand batchRequest = RemotingCommand.createRequestCommand(RequestCode.COMMON_BATCH_REQUEST, header);

        int totalResponseNum = 20;

        Map<Integer, CompletableFuture<RemotingCommand>> allResults = new HashMap<>();

        Map<Integer, CompletableFuture<RemotingCommand>> doneResults = new HashMap<>();

        Map<Integer, CompletableFuture<RemotingCommand>> undoneResults = new HashMap<>();
        for (int i = 0; i < totalResponseNum; i++) {
            Integer opaque = i;
            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            undoneResults.put(opaque, future);
        }

        allResults.putAll(doneResults);
        allResults.putAll(undoneResults);

        CompletableFuture<RemotingCommand> batchFuture = strategy.mergeResponses(batchRequest, allResults);
        Assert.assertFalse(batchFuture.isDone());

        int chooseOneOpaqueComplete = random.nextInt(totalResponseNum);
        CompletableFuture<RemotingCommand> randomFuture = undoneResults.get(chooseOneOpaqueComplete);
        randomFuture.complete(RemotingCommand.createResponse(chooseOneOpaqueComplete, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));

        Assert.assertTrue(batchFuture.isDone());

        RemotingCommand actualBatchResponse = batchFuture.get();
        assertThat(actualBatchResponse.getOpaque()).isEqualTo(batchRequest.getOpaque());

        List<RemotingCommand> actualChildResponses = RemotingCommand.parseChildren(actualBatchResponse);
        Assert.assertEquals(totalResponseNum, actualChildResponses.size());
    }

}
