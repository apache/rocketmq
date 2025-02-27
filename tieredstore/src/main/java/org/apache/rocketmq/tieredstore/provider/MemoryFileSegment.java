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
package org.apache.rocketmq.tieredstore.provider;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryFileSegment extends FileSegment {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final ByteBuffer memStore;
    protected CompletableFuture<Boolean> blocker;
    protected int size = 0;
    protected boolean checkSize = true;

    public MemoryFileSegment(MessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset, MessageStoreExecutor executor) {

        super(storeConfig, fileType, filePath, baseOffset, executor);
        memStore = ByteBuffer.allocate(10000);
        memStore.position((int) getSize());
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public void createFile() {
    }

    public ByteBuffer getMemStore() {
        return memStore;
    }

    public void setCheckSize(boolean checkSize) {
        this.checkSize = checkSize;
    }

    @Override
    public String getPath() {
        return filePath;
    }

    @Override
    public long getSize() {
        if (checkSize) {
            return 1000;
        }
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        ByteBuffer buffer = memStore.duplicate();
        buffer.position((int) position);
        ByteBuffer slice = buffer.slice();
        slice.limit(length);
        return CompletableFuture.completedFuture(slice);
    }

    @Override
    public CompletableFuture<Boolean> commit0(
        FileSegmentInputStream inputStream, long position, int length, boolean append) {

        try {
            if (blocker != null && !blocker.get()) {
                log.info("Commit Blocker Exception for Memory Test");
                return CompletableFuture.completedFuture(false);
            }

            int len;
            byte[] buffer = new byte[1024];
            while ((len = inputStream.read(buffer)) > 0) {
                memStore.put(buffer, 0, len);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public void destroyFile() {
    }
}
