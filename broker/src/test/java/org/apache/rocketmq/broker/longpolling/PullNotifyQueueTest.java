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
package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.broker.longpolling.PullNotifyQueue.PullNotifyQueueConfig;
import org.junit.Assert;
import org.junit.Test;

public class PullNotifyQueueTest {

    @Test
    public void testDrain1() throws Exception {
        PullNotifyQueueConfig c = new PullNotifyQueueConfig();
        c.setTps(1);
        c.setTpsThreshold(2);
        PullNotifyQueue<Object> queue = new PullNotifyQueue<>(c);
        queue.put(new Object());
        queue.put(new Object());
        Assert.assertEquals(1, queue.drain().size());
        Assert.assertEquals(1, queue.drain().size());
    }

    @Test
    public void testDrain2() throws Exception {
        PullNotifyQueueConfig c = new PullNotifyQueueConfig();
        c.setTps(2);
        c.setTpsThreshold(1);
        c.setMaxBatchSize(2);
        PullNotifyQueue<Object> queue = new PullNotifyQueue<>(c);
        queue.put(new Object());
        queue.put(new Object());
        queue.put(new Object());
        Assert.assertEquals(2, queue.drain().size());
        Assert.assertEquals(1, queue.drain().size());
    }

    @Test
    public void testDrain3() throws Exception {
        PullNotifyQueueConfig c = new PullNotifyQueueConfig();
        c.setTps(2);
        c.setTpsThreshold(1);
        c.setMaxBatchSize(100);
        c.setSuggestAvgBatchEachQueue(2);
        c.setMaxLatencyNs(20_000_000);
        PullNotifyQueue<Object> queue = new PullNotifyQueue<>(c);
        Thread t = new Thread(() -> {
            try {
                queue.put(new Object());
                Thread.sleep(5);
                queue.put(new Object());
                Thread.sleep(5);
                queue.put(new Object());
            } catch (Exception e) {
            }
        });
        t.start();
        Assert.assertEquals(2, queue.drain().size());
        Assert.assertEquals(1, queue.drain().size());
    }


    @Test
    public void testDrain4() throws Exception {
        PullNotifyQueueConfig c = new PullNotifyQueueConfig();
        c.setTps(2);
        c.setTpsThreshold(1);
        c.setMaxBatchSize(100);
        c.setSuggestAvgBatchEachQueue(100);
        c.setMaxLatencyNs(30_000_000);
        PullNotifyQueue<Object> queue = new PullNotifyQueue<>(c);
        Thread t = new Thread(() -> {
            try {
                queue.put(new Object());
                Thread.sleep(20);
                queue.put(new Object());
                Thread.sleep(20);
                queue.put(new Object());
            } catch (Exception e) {
            }
        });
        t.start();
        Assert.assertEquals(2, queue.drain().size());
        Assert.assertEquals(1, queue.drain().size());
    }

    @Test
    public void testUpdateActiveConsumeQueueCount() throws Exception {
        PullNotifyQueueConfig c = new PullNotifyQueueConfig();
        PullNotifyQueue<Object> queue = new PullNotifyQueue<>(c);
        queue.setActiveConsumeQueueRefreshTime(2_000_000);
        Thread.sleep(3);
        queue.updateActiveConsumeQueueCount(1);
        Assert.assertEquals(1, queue.getActiveConsumeQueueCount());
        queue.updateActiveConsumeQueueCount(10);
        Assert.assertEquals(10, queue.getActiveConsumeQueueCount());
        queue.updateActiveConsumeQueueCount(5);
        Assert.assertEquals(10, queue.getActiveConsumeQueueCount());
        Thread.sleep(3);
        queue.updateActiveConsumeQueueCount(2);
        Assert.assertEquals(5, queue.getActiveConsumeQueueCount());
        Thread.sleep(3);
        queue.updateActiveConsumeQueueCount(4);
        Assert.assertEquals(4, queue.getActiveConsumeQueueCount());
    }

}
