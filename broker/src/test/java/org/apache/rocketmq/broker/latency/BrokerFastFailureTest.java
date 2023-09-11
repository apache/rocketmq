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
package org.apache.rocketmq.broker.latency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerFastFailureTest {
    @Test
    public void testCleanExpiredRequestInQueue() throws Exception {
        BrokerFastFailure brokerFastFailure = new BrokerFastFailure(null);

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        brokerFastFailure.cleanExpiredRequestInQueue(queue, 1);
        assertThat(queue.size()).isZero();

        //Normal Runnable
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

            }
        };
        queue.add(runnable);

        assertThat(queue.size()).isEqualTo(1);
        brokerFastFailure.cleanExpiredRequestInQueue(queue, 1);
        assertThat(queue.size()).isEqualTo(1);

        queue.clear();

        //With expired request
        RequestTask expiredRequest = new RequestTask(runnable, null, null);
        queue.add(new FutureTaskExt<>(expiredRequest, null));
        TimeUnit.MILLISECONDS.sleep(100);

        RequestTask requestTask = new RequestTask(runnable, null, null);
        queue.add(new FutureTaskExt<>(requestTask, null));

        assertThat(queue.size()).isEqualTo(2);
        brokerFastFailure.cleanExpiredRequestInQueue(queue, 100);
        assertThat(queue.size()).isEqualTo(1);
        assertThat(((FutureTaskExt) queue.peek()).getRunnable()).isEqualTo(requestTask);
    }

}