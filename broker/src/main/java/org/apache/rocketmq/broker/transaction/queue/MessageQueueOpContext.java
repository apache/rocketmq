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
package org.apache.rocketmq.broker.transaction.queue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write buffer for transaction OP (operation) queue offsets.
 *
 * <p>In the transaction message flow, commit/rollback operations produce OP
 * records that are written to the {@code RMQ_SYS_TRANS_OP_HALF_TOPIC}.
 * Instead of writing each OP individually, offsets are buffered here and
 * flushed in batches to reduce I/O.
 *
 * <p>The {@link #contextQueue} holds batched offset strings, while
 * {@link #totalSize} tracks the accumulated count and
 * {@link #lastWriteTimestamp} controls flush timing.
 */
public class MessageQueueOpContext {
    private AtomicInteger totalSize = new AtomicInteger(0);
    private volatile long lastWriteTimestamp;
    // offset1, offset2, offsetN, ...
    private LinkedBlockingQueue<String> contextQueue;

    // queueLength is 20000, hard coded.
    public MessageQueueOpContext(long timestamp, int queueLength) {
        this.lastWriteTimestamp = timestamp;
        contextQueue = new LinkedBlockingQueue<String>(queueLength);
    }

    public LinkedBlockingQueue<String> getContextQueue() {
        return contextQueue;
    }


    public AtomicInteger getTotalSize() {
        return totalSize;
    }


    public long getLastWriteTimestamp() {
        return lastWriteTimestamp;
    }


    public void setLastWriteTimestamp(long lastWriteTimestamp) {
        this.lastWriteTimestamp = lastWriteTimestamp;
    }
}
