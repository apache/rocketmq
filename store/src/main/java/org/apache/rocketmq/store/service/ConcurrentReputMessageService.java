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
package org.apache.rocketmq.store.service;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class ConcurrentReputMessageService extends ReputMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int BATCH_SIZE = 1024 * 1024 * 4;

    private long batchId = 0;
    private final MainBatchDispatchRequestService mainBatchDispatchRequestService;
    private final DispatchService dispatchService;

    public ConcurrentReputMessageService(DefaultMessageStore messageStore) {
        super(messageStore);
        this.mainBatchDispatchRequestService = new MainBatchDispatchRequestService(messageStore);
        this.dispatchService = new DispatchService(messageStore);
    }

    public void createBatchDispatchRequest(ByteBuffer byteBuffer, int position, int size) {
        if (position < 0) {
            return;
        }
        messageStore.getMappedPageHoldCount().getAndIncrement();
        BatchDispatchRequest task = new BatchDispatchRequest(byteBuffer.duplicate(), position, size, batchId++);
        messageStore.getBatchDispatchRequestQueue().offer(task);
    }

    @Override
    public void start() {
        super.start();
        this.mainBatchDispatchRequestService.start();
        this.dispatchService.start();
    }

    @Override
    public void doReput() {
        if (this.reputFromOffset < messageStore.getCommitLog().getMinOffset()) {
            LOGGER.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                this.reputFromOffset, messageStore.getCommitLog().getMinOffset());
            this.reputFromOffset = messageStore.getCommitLog().getMinOffset();
        }
        for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

            SelectMappedBufferResult result = messageStore.getCommitLog().getData(reputFromOffset);

            if (result == null) {
                break;
            }

            int batchDispatchRequestStart = -1;
            int batchDispatchRequestSize = -1;
            try {
                this.reputFromOffset = result.getStartOffset();

                for (int readSize = 0; readSize < result.getSize() && reputFromOffset < messageStore.getConfirmOffset() && doNext; ) {
                    ByteBuffer byteBuffer = result.getByteBuffer();

                    int totalSize = preCheckMessageAndReturnSize(byteBuffer);

                    if (totalSize > 0) {
                        if (batchDispatchRequestStart == -1) {
                            batchDispatchRequestStart = byteBuffer.position();
                            batchDispatchRequestSize = 0;
                        }
                        batchDispatchRequestSize += totalSize;
                        if (batchDispatchRequestSize > BATCH_SIZE) {
                            this.createBatchDispatchRequest(byteBuffer, batchDispatchRequestStart, batchDispatchRequestSize);
                            batchDispatchRequestStart = -1;
                            batchDispatchRequestSize = -1;
                        }
                        byteBuffer.position(byteBuffer.position() + totalSize);
                        this.reputFromOffset += totalSize;
                        readSize += totalSize;
                    } else {
                        doNext = false;
                        if (totalSize == 0) {
                            this.reputFromOffset = messageStore.getCommitLog().rollNextFile(this.reputFromOffset);
                        }
                        this.createBatchDispatchRequest(byteBuffer, batchDispatchRequestStart, batchDispatchRequestSize);
                        batchDispatchRequestStart = -1;
                        batchDispatchRequestSize = -1;
                    }
                }
            } finally {
                this.createBatchDispatchRequest(result.getByteBuffer(), batchDispatchRequestStart, batchDispatchRequestSize);
                boolean over = messageStore.getMappedPageHoldCount().get() == 0;
                while (!over) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    over = messageStore.getMappedPageHoldCount().get() == 0;
                }
                result.release();
            }
        }
    }

    /**
     * pre-check the message and returns the message size
     *
     * @return 0 Come to the end of file // >0 Normal messages // -1 Message checksum failure
     */
    public int preCheckMessageAndReturnSize(ByteBuffer byteBuffer) {
        byteBuffer.mark();

        int totalSize = byteBuffer.getInt();
        if (reputFromOffset + totalSize > messageStore.getConfirmOffset()) {
            return -1;
        }

        int magicCode = byteBuffer.getInt();
        switch (magicCode) {
            case MessageDecoder.MESSAGE_MAGIC_CODE:
            case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                break;
            case MessageDecoder.BLANK_MAGIC_CODE:
                return 0;
            default:
                return -1;
        }

        byteBuffer.reset();

        return totalSize;
    }

    @Override
    public void shutdown() {
        for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }

        if (this.isCommitLogAvailable()) {
            LOGGER.warn("shutdown concurrentReputMessageService, but CommitLog have not finish to be dispatched, CommitLog max" +
                    " offset={}, reputFromOffset={}", messageStore.getCommitLog().getMaxOffset(),
                this.reputFromOffset);
        }

        this.mainBatchDispatchRequestService.shutdown();
        this.dispatchService.shutdown();
        super.shutdown();
    }

    @Override
    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + ConcurrentReputMessageService.class.getSimpleName();
        }
        return ConcurrentReputMessageService.class.getSimpleName();
    }
}

