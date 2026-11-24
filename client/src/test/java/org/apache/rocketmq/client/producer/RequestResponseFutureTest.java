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

package org.apache.rocketmq.client.producer;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RequestResponseFutureTest {

    @Test
    public void testExecuteRequestCallback() throws Exception {
        final AtomicInteger cc = new AtomicInteger(0);
        RequestResponseFuture future = new RequestResponseFuture(UUID.randomUUID().toString(), 3 * 1000L, new RequestCallback() {
            @Override
            public void onSuccess(Message message) {
                cc.incrementAndGet();
            }

            @Override
            public void onException(Throwable e) {
            }
        });
        future.setSendRequestOk(true);
        future.executeRequestCallback();
        assertThat(cc.get()).isEqualTo(1);
    }

    @Test
    public void testExecuteRequestCallbackSuccessThenTimeoutFiresOnce() {
        final AtomicInteger success = new AtomicInteger(0);
        final AtomicInteger exception = new AtomicInteger(0);
        RequestResponseFuture future = new RequestResponseFuture(UUID.randomUUID().toString(), 3 * 1000L, new RequestCallback() {
            @Override
            public void onSuccess(Message message) {
                success.incrementAndGet();
            }

            @Override
            public void onException(Throwable e) {
                exception.incrementAndGet();
            }
        });

        // Reply-arrival path wins first (success), then the timeout-scan path tries to fire again.
        future.setSendRequestOk(true);
        future.executeRequestCallback();

        future.setCause(new RuntimeException("request timeout, no reply message."));
        future.executeRequestCallback();

        // The same request must never deliver both a success and a timeout callback.
        assertThat(success.get()).isEqualTo(1);
        assertThat(exception.get()).isEqualTo(0);
    }

    @Test
    public void testExecuteRequestCallbackConcurrentlyFiresOnce() throws Exception {
        final AtomicInteger total = new AtomicInteger(0);
        final RequestResponseFuture future = new RequestResponseFuture(UUID.randomUUID().toString(), 3 * 1000L, new RequestCallback() {
            @Override
            public void onSuccess(Message message) {
                total.incrementAndGet();
            }

            @Override
            public void onException(Throwable e) {
                total.incrementAndGet();
            }
        });
        future.setSendRequestOk(true);

        int threads = 16;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    future.executeRequestCallback();
                } catch (InterruptedException ignored) {
                } finally {
                    done.countDown();
                }
            }).start();
        }
        start.countDown();
        done.await();

        // Under concurrent contention the CAS guard still allows exactly one callback.
        assertThat(total.get()).isEqualTo(1);
    }

}
