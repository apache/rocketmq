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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.impl.producer.DefaultSendPromise;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestSendPromise {

    @Test
    public void testAddAndRemoveCallbacks() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        DefaultSendPromise promise = new DefaultSendPromise(executor);
        final CountDownLatch latch = new CountDownLatch(1);
        SendCallback callback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                latch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                latch.countDown();
            }
        };
        SendCallback callback1 = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                Assert.fail();
            }

            @Override
            public void onException(Throwable e) {
                Assert.fail();
            }
        };

        promise.addCallback(callback);
        promise.removeCallback(callback);
        promise.complete(new SendResult());
        Assert.assertEquals(latch.getCount(), 1);

        promise = new DefaultSendPromise(executor);
        promise.addCallback(callback);
        promise.addCallback(callback1);
        promise.removeCallback(callback1);
        promise.complete(new SendResult());
        Assert.assertTrue(latch.await(1000, TimeUnit.SECONDS));
        Assert.assertTrue(promise.isDone());

        executor.shutdown();
    }

    @Test
    public void testBasicOperations() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        DefaultSendPromise promise = new DefaultSendPromise(executor);

        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch latch1 = new CountDownLatch(2);
        SendCallback callback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                latch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                latch1.countDown();
            }
        };
        SendCallback callback2 = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                latch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                latch1.countDown();
            }
        };
        promise.addCallback(callback).addCallback(callback2);

        SendResult result = new SendResult();
        promise.complete(result);
        Assert.assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
        Assert.assertEquals(promise.get(), result);
        Assert.assertEquals(promise.get(1, TimeUnit.SECONDS), result);

        DefaultSendPromise promise1 = new DefaultSendPromise(executor);
        Exception cause = new Exception();
        promise1.report(cause);
        promise1.addCallback(callback).addCallback(callback2);

        Assert.assertEquals(cause, promise1.getCause());

        Assert.assertTrue(latch1.await(5000, TimeUnit.MILLISECONDS));

        executor.shutdown();
    }
}
