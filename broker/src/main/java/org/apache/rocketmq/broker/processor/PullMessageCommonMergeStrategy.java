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
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.pagecache.BatchManyMessageTransfer;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.CommonBatchResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.rocketmq.common.protocol.ResponseCode.PULL_NOT_FOUND;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;
import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SYSTEM_ERROR;

public class PullMessageCommonMergeStrategy extends MergeBatchResponseStrategy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final PullMessageCommonMergeStrategy INSTANCE = new PullMessageCommonMergeStrategy();

    private PullMessageCommonMergeStrategy() {
    }

    @Override
    public CompletableFuture<RemotingCommand> mergeResponses(
            RemotingCommand batchRequest,
            Map<Integer, CompletableFuture<RemotingCommand>> opaqueToFuture) {

        Preconditions.checkNotNull(batchRequest, "batchRequest shouldn't be null.");
        Preconditions.checkNotNull(opaqueToFuture, "opaqueToFuture shouldn't be null.");
        Preconditions.checkArgument(!opaqueToFuture.isEmpty());

        final int batchOpaque = batchRequest.getOpaque();

        final CompletableFuture<RemotingCommand> batchFuture = new CompletableFuture<>();

        final int expectedResponseNum = opaqueToFuture.size();

        AtomicBoolean completeStat = new AtomicBoolean(false);
        ConcurrentHashMap<Integer, RemotingCommand> doneResults = new ConcurrentHashMap<>(expectedResponseNum);
        ConcurrentHashMap<Integer, CompletableFuture<RemotingCommand>> undoneResults = new ConcurrentHashMap<>();

        opaqueToFuture.forEach((childOpaque, childFuture) -> childFuture.whenComplete((childResp, throwable) -> {
            if (completeStat.compareAndSet(false, true)) {
                opaqueToFuture.forEach((cOpaque, cFuture) -> {
                    if (cFuture.isDone()) {
                        doneResults.put(cOpaque, nonNullableResponse(cOpaque, extractResult(batchRequest, cOpaque, cFuture)));
                    } else {
                        undoneResults.put(cOpaque, cFuture);
                    }
                });
                completeUndoneResults(batchRequest, undoneResults);
            } else {
                doneResults.put(childOpaque, nonNullableResponse(childOpaque, childResp));
            }

            completeBatchFuture(batchFuture, doneResults, expectedResponseNum, batchOpaque, childOpaque, batchRequest);
        }));

        return batchFuture;
    }

    @Override
    protected RemotingCommand doMerge(
            List<RemotingCommand> responses,
            int expectedResponseNum,
            int batchOpaque,
            int childOpaque,
            RemotingCommand batchRequest) {
        if (isZeroCopy(responses)) {
            return this.doMergeZeroCopy(responses, batchOpaque, childOpaque, batchRequest);
        } else {
            return super.doMerge(responses, expectedResponseNum, batchOpaque, childOpaque, batchRequest);
        }
    }

    private boolean isZeroCopy(List<RemotingCommand> responses) {
        List<RemotingCommand> withAttachment = new ArrayList<>();
        List<RemotingCommand> withoutAttachment = new ArrayList<>();
        List<RemotingCommand> withoutAttachmentHavingNoData = new ArrayList<>();

        for (RemotingCommand response : responses) {
            if (response.getFileRegionAttachment() instanceof FileRegion) {
                withAttachment.add(response);
            } else {
                withoutAttachment.add(response);
                if (response.getCode() == PULL_NOT_FOUND) {
                    withoutAttachmentHavingNoData.add(response);
                }
            }
        }

        if (withoutAttachment.size() == responses.size()) {
            return false;
        }

        if (withAttachment.size() == responses.size()) {
            return true;
        }

        // zero copy + partial long-polling
        if (!withAttachment.isEmpty() && withoutAttachmentHavingNoData.size() == withoutAttachment.size()) {
            return true;
        } else {
            withAttachment.forEach(childResp -> {
                if (childResp.getFinallyReleasingCallback() != null) {
                    childResp.getFinallyReleasingCallback().run();
                }
            });
            throw new RuntimeException("inconsistent config: transfer by heap.");
        }
    }

    private void completeUndoneResults(RemotingCommand batchRequest, Map<Integer, CompletableFuture<RemotingCommand>> undoneResults) {
        if (undoneResults.isEmpty()) {
            return ;
        }
        // complete it with a PULL_NOT_FOUND value.
        undoneResults.forEach((childOpaque, undoneResult) ->
                MergeBatchResponseStrategy.completeUndoneResult(batchRequest, childOpaque, undoneResult, PULL_NOT_FOUND, REMARK_PULL_NOT_FOUND));
    }

    private RemotingCommand doMergeZeroCopy(List<RemotingCommand> responses, int batchOpaque, int childOpaque, RemotingCommand batchRequest) {
        Preconditions.checkArgument(!responses.isEmpty());

        CommonBatchResponseHeader commonBatchResponseHeader = new CommonBatchResponseHeader();
        try {
            RemotingCommand zeroCopyResponse = RemotingCommand.createResponse(batchOpaque, SUCCESS, REMARK_SUCCESS);
            List<ManyMessageTransfer> manyMessageTransferList = new ArrayList<>();

            int bodyLength = 0;
            for (RemotingCommand resp : responses) {
                if (resp.getFileRegionAttachment() == null) {
                    // responses whose attachment is null are allowed to be omitted from broker
                    continue;
                }
                ManyMessageTransfer manyMessageTransfer = (ManyMessageTransfer) resp.getFileRegionAttachment();
                manyMessageTransferList.add(manyMessageTransfer);

                bodyLength += manyMessageTransfer.count();
            }

            zeroCopyResponse.setHeader(commonBatchResponseHeader);

            ByteBuffer batchHeader = zeroCopyResponse.encodeHeader(bodyLength);
            BatchManyMessageTransfer batchManyMessageTransfer = new BatchManyMessageTransfer(batchHeader, manyMessageTransferList);

            Runnable releaseBatch = batchManyMessageTransfer::close;
            zeroCopyResponse.setFileRegionAttachment(batchManyMessageTransfer);
            zeroCopyResponse.setFinallyReleasingCallback(releaseBatch);

            return zeroCopyResponse;
        } catch (Exception e) {
            log.error("doMergeZeroCopy for batch-request {} failed in childOpaque {}.", batchRequest, childOpaque, e);
            for (RemotingCommand resp : responses) {
                if (resp.getFileRegionAttachment() != null) {
                    ((ManyMessageTransfer) resp.getFileRegionAttachment()).close();
                }
            }
            RemotingCommand error = RemotingCommand.createResponse(batchOpaque, SYSTEM_ERROR, REMARK_ZERO_COPY_SYSTEM_ERROR);
            error.setHeader(commonBatchResponseHeader);

            return error;
        }
    }

    public static PullMessageCommonMergeStrategy getInstance() {
        return INSTANCE;
    }
}
