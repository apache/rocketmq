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
package org.apache.rocketmq.tieredstore.provider.memory;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.Assert;

public class MemoryFileSegmentWithoutCheck extends MemoryFileSegment {

    public MemoryFileSegmentWithoutCheck(FileSegmentType fileType,
        MessageQueue messageQueue, long baseOffset, TieredMessageStoreConfig storeConfig) {
        super(storeConfig, fileType,
            storeConfig.getStorePathRootDir() + File.separator + TieredStoreUtil.toPath(messageQueue),
            baseOffset);
    }

    public MemoryFileSegmentWithoutCheck(TieredMessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {
        super(storeConfig, fileType, filePath, baseOffset);
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public CompletableFuture<Boolean> commit0(TieredFileSegmentInputStream inputStream, long position, int length,
        boolean append) {
        try {
            if (blocker != null && !blocker.get()) {
                throw new IllegalStateException();
            }
        } catch (InterruptedException | ExecutionException e) {
            Assert.fail(e.getMessage());
        }

        byte[] buffer = new byte[1024];

        int startPos = memStore.position();
        try {
            int len;
            while ((len = inputStream.read(buffer)) > 0) {
                memStore.put(buffer, 0, len);
            }
            Assert.assertEquals(length, memStore.position() - startPos);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.completedFuture(true);
    }
}
