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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.utils.ThreadUtils;

public abstract class AbstractConsumeMessageService implements ConsumeMessageService {
    protected final DefaultMQPushConsumer defaultMQPushConsumer;
    protected final ExecutorService consumeExecutor;
    private final boolean ownsConsumeExecutor;

    protected AbstractConsumeMessageService(DefaultMQPushConsumer defaultMQPushConsumer, ThreadFactory threadFactory) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        ExecutorService externalExecutor = defaultMQPushConsumer.getConsumeExecutor();
        this.ownsConsumeExecutor = externalExecutor == null;
        if (this.ownsConsumeExecutor) {
            this.consumeExecutor = new ThreadPoolExecutor(
                defaultMQPushConsumer.getConsumeThreadMin(),
                defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory);
        } else {
            this.consumeExecutor = externalExecutor;
        }
    }

    protected static String getConsumerGroupTag(String consumerGroup) {
        return (consumerGroup.length() > 100 ? consumerGroup.substring(0, 100) : consumerGroup) + "_";
    }

    protected void shutdownConsumeExecutor(long awaitTerminateMillis) {
        if (this.ownsConsumeExecutor) {
            ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (this.ownsConsumeExecutor
            && corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            ((ThreadPoolExecutor) this.consumeExecutor).setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.ownsConsumeExecutor ? ((ThreadPoolExecutor) this.consumeExecutor).getCorePoolSize() : -1;
    }
}
