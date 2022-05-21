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

package org.apache.rocketmq.thinclient.impl.consumer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.thinclient.hook.MessageInterceptor;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;

@SuppressWarnings("NullableProblems")
public class StandardConsumeService extends ConsumeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardConsumeService.class);

    public StandardConsumeService(String clientId, ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable,
        MessageListener messageListener, ThreadPoolExecutor consumptionExecutor, MessageInterceptor messageInterceptor,
        ScheduledExecutorService scheduler) {
        super(clientId, processQueueTable, messageListener, consumptionExecutor, messageInterceptor, scheduler);
    }

    /**
     * dispatch message(s) once
     *
     * @return if message is dispatched.
     */
    public boolean dispatch0() {
        final List<ProcessQueue> processQueues = new ArrayList<>(processQueueTable.values());
        // Shuffle all process queue in case messages are always consumed firstly in one message queue.
        Collections.shuffle(processQueues);
        boolean dispatched = false;
        // Iterate all process queues to submit consumption task.
        for (ProcessQueue pq : processQueues) {
            final Optional<MessageViewImpl> optionalMessageView = pq.tryTakeMessage();
            if (!optionalMessageView.isPresent()) {
                continue;
            }
            dispatched = true;
            MessageViewImpl messageView = optionalMessageView.get();
            final ListenableFuture<ConsumeResult> future = consume(messageView);
            Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
                @Override
                public void onSuccess(ConsumeResult consumeResult) {
                    pq.eraseMessage(messageView, consumeResult);
                }

                @Override
                public void onFailure(Throwable t) {
                    // Should never reach here.
                    LOGGER.error("[Bug] Exception raised in consumption callback, clientId={}", clientId, t);
                }
            }, MoreExecutors.directExecutor());
        }
        return dispatched;
    }

    /**
     * Loop of message dispatch.
     */
    @Override
    public void dispatch() {
        boolean dispatched;
        do {
            dispatched = dispatch0();
        }
        while (dispatched);
    }
}
