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
package org.apache.rocketmq.proxy.service.sysmessage;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.config.ProxyConfig;

/** Creates the executor shared by a Proxy's internal system-message consumers. */
public class SystemMessageConsumeExecutor {
    private SystemMessageConsumeExecutor() {
    }

    public static ThreadPoolExecutor create(ProxyConfig config) {
        ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
            config.getSystemMessageConsumerThreadPoolCoreSize(),
            config.getSystemMessageConsumerThreadPoolMaxSize(),
            1, TimeUnit.MINUTES, "SystemMessageConsumer",
            config.getSystemMessageConsumerThreadPoolQueueCapacity(),
            new DiscardOldestPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /** Discard the oldest queued task and complete its consumer-local lifecycle bookkeeping. */
    static class DiscardOldestPolicy extends ThreadPoolExecutor.DiscardOldestPolicy {
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if (executor.isShutdown()) {
                throw new RejectedExecutionException("System-message consumption executor has stopped");
            }
            Runnable discarded = executor.getQueue().poll();
            try {
                if (discarded instanceof FutureTaskExt<?>) {
                    Runnable command = ((FutureTaskExt<?>) discarded).getRunnable();
                    if (command instanceof ConsumeRequest) {
                        ConsumeRequest request = (ConsumeRequest) command;
                        if (!request.getProcessQueue().isDropped()) {
                            // These consumers use BROADCASTING. Apply its existing failure cleanup
                            // here, rather than changing rejection behavior for ordinary clients.
                            request.getConsumeMessageService().processConsumeResult(
                                ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                new ConsumeConcurrentlyContext(request.getMessageQueue()), request);
                        }
                    }
                }
            } finally {
                if (discarded instanceof Future<?>) {
                    ((Future<?>) discarded).cancel(false);
                }
            }
            executor.execute(task);
        }
    }
}
