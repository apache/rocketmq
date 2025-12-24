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
package org.apache.rocketmq.tieredstore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;

public class MessageStoreExecutorTest {

    private MessageStoreExecutor executor;

    @Before
    public void setUp() {
        executor = new MessageStoreExecutor(16);
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void testFileRecyclingExecutorThreadNamePrefix() throws Exception {
        assertThreadNamePrefix(executor.fileRecyclingExecutor, "FileRecyclingExecutor_",
            "fileRecyclingExecutor");
    }

    @Test
    public void testBufferCommitExecutorThreadNamePrefix() throws Exception {
        assertThreadNamePrefix(executor.bufferCommitExecutor, "BufferCommitExecutor_",
            "bufferCommitExecutor");
    }

    @Test
    public void testBufferFetchExecutorThreadNamePrefix() throws Exception {
        assertThreadNamePrefix(executor.bufferFetchExecutor, "BufferFetchExecutor_",
            "bufferFetchExecutor");
    }

    @Test
    public void testCommonExecutorThreadNamePrefix() throws Exception {
        assertThreadNamePrefix(executor.commonExecutor, "TieredCommonExecutor_",
            "commonExecutor");
    }

    private void assertThreadNamePrefix(Executor executor, String expectedPrefix, String executorName) throws Exception {
        String threadName = getThreadNameFromExecutor(executor);
        Assert.assertNotNull("Thread name should not be null", threadName);
        Assert.assertTrue(
            executorName + " thread name should start with " + expectedPrefix + " but was: " + threadName,
            threadName.startsWith(expectedPrefix));
    }

    private String getThreadNameFromExecutor(Executor executor) throws Exception {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(
            () -> Thread.currentThread().getName(),
            executor);

        return future.get(5, TimeUnit.SECONDS);
    }
}

