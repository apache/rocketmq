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
package org.apache.rocketmq.store.logfile;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Shared byte buffer manager for managing some shared ByteBuffers Buffer size is set based on MessageStoreConfig's
 * maxMessageSize
 */
public class SharedByteBufferManager {

    private static volatile SharedByteBufferManager instance;
    private static final Object LOCK = new Object();

    private SharedByteBuffer[] sharedByteBuffers;
    private int bufferSize;
    private int maxSharedNum;
    private volatile boolean initialized = false;

    private SharedByteBufferManager() {
        // Private constructor
    }

    /**
     * Get singleton instance
     */
    public static SharedByteBufferManager getInstance() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new SharedByteBufferManager();
                }
            }
        }
        return instance;
    }

    /**
     * Initialize shared buffers with specified messageSize size and shared buffer number
     *
     * @param maxMessageSize max messageSize size
     * @param sharedBufferNum number of shared buffers
     */
    public synchronized void init(int maxMessageSize, int sharedBufferNum) {
        if (!initialized) {
            //Reserve 64kb for encoding buffer outside body
            bufferSize = Integer.MAX_VALUE - maxMessageSize >= 64 * 1024 ?
                maxMessageSize + 64 * 1024 : Integer.MAX_VALUE;

            this.maxSharedNum = sharedBufferNum;
            this.sharedByteBuffers = new SharedByteBuffer[maxSharedNum];
            for (int i = 0; i < maxSharedNum; i++) {
                this.sharedByteBuffers[i] = new SharedByteBuffer(bufferSize);
            }
            this.initialized = true;
        }
    }

    /**
     * Borrow a shared buffer
     *
     * @return Shared buffer
     */
    public SharedByteBuffer borrowSharedByteBuffer() {
        if (!initialized) {
            throw new IllegalStateException("SharedByteBufferManager not initialized");
        }
        int idx = ThreadLocalRandom.current().nextInt(maxSharedNum);
        return sharedByteBuffers[idx];
    }

    /**
     * Get current buffer size
     *
     * @return Buffer size
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Check if initialized
     *
     * @return Whether initialized
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Shared byte buffer class
     */
    public static class SharedByteBuffer {
        private final ReentrantLock lock;
        private final ByteBuffer buffer;

        public SharedByteBuffer(int size) {
            this.lock = new ReentrantLock();
            this.buffer = ByteBuffer.allocateDirect(size);
        }

        public void release() {
            this.lock.unlock();
        }

        public ByteBuffer acquire() {
            this.lock.lock();
            return buffer;
        }
    }
}
