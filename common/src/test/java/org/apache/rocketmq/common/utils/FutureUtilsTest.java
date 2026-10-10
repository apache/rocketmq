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

package org.apache.rocketmq.common.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FutureUtilsTest {

    @Test
    public void testAddExecutorCompletesExceptionallyWhenExecutorRejectsTask() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.shutdown();

        CompletableFuture<String> result = FutureUtils.addExecutor(
            CompletableFuture.completedFuture("value"), executor);

        assertTrue("the returned future must not remain pending", result.isDone());
        assertTrue(result.isCompletedExceptionally());
        CompletionException exception = assertThrows(CompletionException.class, () -> result.join());
        assertTrue(exception.getCause() instanceof RejectedExecutionException);
    }
}
