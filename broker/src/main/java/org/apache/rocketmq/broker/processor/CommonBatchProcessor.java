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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.CommonBatchRequestHeader;
import org.apache.rocketmq.common.protocol.header.CommonBatchResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public class CommonBatchProcessor extends AsyncNettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public static final String REMARK_SYSTEM_ERROR = "system error in processor";

    private final BrokerController brokerController;

    public CommonBatchProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        throw new RuntimeException("not supported.");
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    @Override
    public CompletableFuture<RemotingCommand> asyncProcessRequest(
            ChannelHandlerContext ctx,
            RemotingCommand batchRequest,
            RemotingResponseCallback responseCallback) {
        log.debug("receive common-batch request command, {}", batchRequest);

        Map<Integer /* opaque */, CompletableFuture<RemotingCommand>> opaqueToFuture = new HashMap<>();
        try {
            AsyncNettyRequestProcessor asyncNettyRequestProcessor = dispatchProcessor(batchRequest);
            List<RemotingCommand> requestChildren = RemotingCommand.parseChildren(batchRequest);
            if (tooManyChildren(requestChildren)) {
                log.warn("there are too many children ({}) in a batch request {}.", requestChildren.size(), batchRequest);
            }

            for (RemotingCommand childRequest : requestChildren) {
                CompletableFuture<RemotingCommand> childFuture = asyncNettyRequestProcessor.asyncProcessRequest(ctx, childRequest, responseCallback);
                int opaque = childRequest.getOpaque();
                opaqueToFuture.put(opaque, childFuture);
            }

            MergeBatchResponseStrategy strategy = selectStrategy(asyncNettyRequestProcessor);
            return strategy.mergeResponses(batchRequest, opaqueToFuture);
        } catch (Exception e) {
            log.error("asyncProcessRequest {} failed.", batchRequest, e);

            releasingReferenceCounted(batchRequest, opaqueToFuture);
            RemotingCommand errorBatchResponse = RemotingCommand.createResponse(batchRequest.getOpaque(), SYSTEM_ERROR, REMARK_SYSTEM_ERROR);
            CommonBatchResponseHeader commonBatchResponseHeader = new CommonBatchResponseHeader();
            errorBatchResponse.setHeader(commonBatchResponseHeader);
            return CompletableFuture.completedFuture(errorBatchResponse);
        }
    }

    private boolean tooManyChildren(List<RemotingCommand> requestChildren) {
        return requestChildren.size() > this.brokerController.getBrokerConfig().getMaxChildRequestNum();
    }

    private void releasingReferenceCounted(RemotingCommand batchRequest, Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture) {
        Map<Integer, RemotingCommand> doneResults = new HashMap<>();
        Map<Integer, CompletableFuture<RemotingCommand>> undoneResults = new HashMap<>();

        opaqueToFuture.forEach((cOpaque, cFuture) -> {
            if (cFuture.isDone()) {
                doneResults.put(cOpaque, MergeBatchResponseStrategy.extractResult(batchRequest, cOpaque, cFuture));
            } else {
                undoneResults.put(cOpaque, cFuture);
            }
        });
        undoneResults.forEach((childOpaque, undoneResult) ->
                MergeBatchResponseStrategy.completeUndoneResult(batchRequest, childOpaque, undoneResult, SYSTEM_ERROR, REMARK_SYSTEM_ERROR));
        doneResults.forEach((childOpaque, response) -> {
            if (response != null && response.getFinallyReleasingCallback() != null) {
                response.getFinallyReleasingCallback().run();
            }
        });
    }

    private MergeBatchResponseStrategy selectStrategy(AsyncNettyRequestProcessor asyncNettyRequestProcessor) {
        if (asyncNettyRequestProcessor instanceof PullMessageProcessor) {
            return PullMessageCommonMergeStrategy.getInstance();
        } else {
            return CommonMergeBatchResponseStrategy.getInstance();
        }
    }

    private AsyncNettyRequestProcessor dispatchProcessor(RemotingCommand batchRequest) throws RemotingCommandException {
        final CommonBatchRequestHeader batchHeader =
                (CommonBatchRequestHeader) batchRequest.decodeCommandCustomHeader(CommonBatchRequestHeader.class);

        int dispatchCode = batchHeader.getDispatchCode();

        Pair<NettyRequestProcessor, ExecutorService> processorPair = this.brokerController.getRemotingServer().getProcessorPair(dispatchCode);
        if (processorPair == null) {
            throw new RuntimeException("dispatchCode " + dispatchCode + " is not supported.");
        }
        return (AsyncNettyRequestProcessor) processorPair.getObject1();
    }
}
