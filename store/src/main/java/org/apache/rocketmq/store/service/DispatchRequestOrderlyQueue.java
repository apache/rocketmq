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
package org.apache.rocketmq.store.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.store.DispatchRequest;

public class DispatchRequestOrderlyQueue {
    DispatchRequest[][] buffer;
    long ptr = 0;
    AtomicLong maxPtr = new AtomicLong();

    public DispatchRequestOrderlyQueue(int bufferNum) {
        this.buffer = new DispatchRequest[bufferNum][];
    }

    public void put(long index, DispatchRequest[] dispatchRequests) {
        while (ptr + this.buffer.length <= index) {
            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        int mod = (int) (index % this.buffer.length);
        this.buffer[mod] = dispatchRequests;
        maxPtr.incrementAndGet();
    }

    public DispatchRequest[] get(List<DispatchRequest[]> dispatchRequestsList) {
        synchronized (this) {
            for (int i = 0; i < this.buffer.length; i++) {
                int mod = (int) (ptr % this.buffer.length);
                DispatchRequest[] ret = this.buffer[mod];
                if (ret == null) {
                    this.notifyAll();
                    return null;
                }
                dispatchRequestsList.add(ret);
                this.buffer[mod] = null;
                ptr++;
            }
        }
        return null;
    }

    public synchronized boolean isEmpty() {
        return maxPtr.get() == ptr;
    }

}

