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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumeMessageConcurrentlyServiceThreadPoolTest {

    @Test
    public void testConsumeThreadMaxAllowsPoolToGrowAboveMin() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("threadPoolTestGroup");
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(2);
        DefaultMQPushConsumerImpl consumerImpl = mock(DefaultMQPushConsumerImpl.class);
        when(consumerImpl.getDefaultMQPushConsumer()).thenReturn(consumer);
        MessageListenerConcurrently listener = (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        ConsumeMessageConcurrentlyService service = new ConsumeMessageConcurrentlyService(consumerImpl, listener);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) service.consumeExecutor;
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Runnable blockingTask = () -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        try {
            executor.execute(blockingTask);
            executor.execute(blockingTask);
            executor.execute(blockingTask);

            assertTrue("Pool did not grow to consumeThreadMax", started.await(5, TimeUnit.SECONDS));
            assertEquals(2, executor.getPoolSize());
        } finally {
            release.countDown();
            service.shutdown(5000);
        }
    }
}
