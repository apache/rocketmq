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
package org.apache.rocketmq.broker.api;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerTestHarness;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.latency.FutureTaskExt;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.junit.Assert;
import org.junit.Test;

public class BrokerFastFailureTest extends BrokerTestHarness {

    @Test
    public void testHeadSlowTimeMills() throws InterruptedException {
        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>();
        blockingQueue.add(new FutureTaskExt<>(new RequestTask(null, null, null), null));
        TimeUnit.MILLISECONDS.sleep(10);
        Assert.assertTrue(this.brokerController.headSlowTimeMills(blockingQueue) > 0);

        blockingQueue.clear();
        blockingQueue.add(new Runnable() {
            @Override public void run() {

            }
        });
        Assert.assertTrue(this.brokerController.headSlowTimeMills(blockingQueue) == 0);
    }

    @Test
    public void testCastRunnable() {
        Runnable runnable = new Runnable() {
            @Override public void run() {

            }
        };
        Assert.assertNull(BrokerFastFailure.castRunnable(runnable));

        RequestTask requestTask = new RequestTask(null, null, null);
        runnable = new FutureTaskExt<>(requestTask, null);

        Assert.assertEquals(requestTask, BrokerFastFailure.castRunnable(runnable));
    }
}
