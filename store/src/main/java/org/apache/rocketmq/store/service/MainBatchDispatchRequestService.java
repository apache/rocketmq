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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;

public class MainBatchDispatchRequestService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore messageStore;
    private final ExecutorService batchDispatchRequestExecutor;

    public MainBatchDispatchRequestService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;

        batchDispatchRequestExecutor = new ThreadPoolExecutor(
            messageStore.getMessageStoreConfig().getBatchDispatchRequestThreadPoolNums(),
            messageStore.getMessageStoreConfig().getBatchDispatchRequestThreadPoolNums(),
            1000 * 60,
            TimeUnit.MICROSECONDS,
            new LinkedBlockingQueue<>(4096),
            new ThreadFactoryImpl("BatchDispatchRequestServiceThread_"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    private void pollBatchDispatchRequest() {
        try {
            if (!messageStore.getBatchDispatchRequestQueue().isEmpty()) {
                BatchDispatchRequest task = messageStore.getBatchDispatchRequestQueue().peek();
                batchDispatchRequestExecutor.execute(() -> {
                    try {
                        ByteBuffer tmpByteBuffer = task.getByteBuffer();
                        tmpByteBuffer.position(task.getPosition());
                        tmpByteBuffer.limit(task.getPosition() + task.getSize());
                        List<DispatchRequest> dispatchRequestList = new ArrayList<>();
                        while (tmpByteBuffer.hasRemaining()) {
                            DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(tmpByteBuffer, false, false, false);
                            if (dispatchRequest.isSuccess()) {
                                dispatchRequestList.add(dispatchRequest);
                            } else {
                                LOGGER.error("[BUG]read total count not equals msg total size.");
                            }
                        }
                        messageStore.getDispatchRequestOrderlyQueue().put(task.getId(), dispatchRequestList.toArray(new DispatchRequest[dispatchRequestList.size()]));
                        messageStore.getMappedPageHoldCount().getAndDecrement();
                    } catch (Exception e) {
                        LOGGER.error("There is an exception in task execution.", e);
                    }
                });
                messageStore.getBatchDispatchRequestQueue().poll();
            }
        } catch (Exception e) {
            LOGGER.warn(this.getServiceName() + " service has exception. ", e);
        }
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                pollBatchDispatchRequest();
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        LOGGER.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + MainBatchDispatchRequestService.class.getSimpleName();
        }
        return MainBatchDispatchRequestService.class.getSimpleName();
    }

}

