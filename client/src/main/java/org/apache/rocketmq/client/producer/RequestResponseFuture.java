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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.message.Message;

public class RequestResponseFuture {
    private final String correlationId;
    private final RequestCallback requestCallback;
    private final long beginTimestamp = System.currentTimeMillis();
    private long timeoutMillis;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private volatile Message responseMsg = null;
    private volatile boolean sendRequestOk = false;
    private volatile Throwable cause = null;

    private AtomicInteger callbackCounter = new AtomicInteger(0);

    public RequestResponseFuture(String correlationId, long timeoutMillis, RequestCallback requestCallback) {
        this.correlationId = correlationId;
        this.timeoutMillis = timeoutMillis;
        this.requestCallback = requestCallback;
    }

    public void onSuccess(Message msg) {
        if (!this.isSupportMultiCallback()) {
            if (!this.callbackCounter.compareAndSet(0, 1)) {
                return;
            }
            this.responseMsg = msg;
            this.countDownLatch.countDown();
            if (this.requestCallback != null) {
                try {
                    this.requestCallback.onSuccess(msg);
                } finally {
                    RequestFutureHolder.getInstance().getRequestFutureTable().remove(this.correlationId);
                }
            }
        } else {
            int count = this.callbackCounter.incrementAndGet();
            if (count == 0) { // exception
                return;
            }
            if (count == 1) {
                this.responseMsg = msg;
                this.countDownLatch.countDown();
            }
            this.requestCallback.onSuccess(msg);
        }
    }

    public void onException(Throwable e) {
        if (!this.callbackCounter.compareAndSet(0, -1)) {
            return;
        }
        this.cause = e;
        this.countDownLatch.countDown();
        try {
            this.requestCallback.onException(e);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(this.correlationId);
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public Message waitResponseMessage(final long timeout) throws InterruptedException {
        this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        return this.responseMsg;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public boolean isSendRequestOk() {
        return sendRequestOk;
    }

    public void setSendRequestOk(boolean sendRequestOk) {
        this.sendRequestOk = sendRequestOk;
    }

    public Throwable getCause() {
        return cause;
    }

    protected boolean isSupportMultiCallback() {
        return requestCallback != null && requestCallback.allowMultipleCallback();
    }
}
