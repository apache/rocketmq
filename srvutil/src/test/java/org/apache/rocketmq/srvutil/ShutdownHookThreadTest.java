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

package org.apache.rocketmq.srvutil;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings("DoNotCall")
public class ShutdownHookThreadTest {

    @Test
    public void testRunShouldInvokeCallbackOnlyOnce() throws Exception {
        Logger logger = Mockito.mock(Logger.class);
        AtomicInteger callbackTimes = new AtomicInteger();
        Callable<Object> callback = () -> {
            callbackTimes.incrementAndGet();
            return null;
        };

        ShutdownHookThread shutdownHookThread = new ShutdownHookThread(logger, callback);

        shutdownHookThread.run();
        shutdownHookThread.run();

        assertThat(callbackTimes.get()).isEqualTo(1);
        verify(logger, times(3)).info(anyString());
    }

    @Test
    public void testRunShouldLogErrorWhenCallbackThrows() throws Exception {
        Logger logger = Mockito.mock(Logger.class);
        AtomicInteger callbackTimes = new AtomicInteger();
        Callable<Object> callback = () -> {
            callbackTimes.incrementAndGet();
            throw new IllegalStateException("boom");
        };

        ShutdownHookThread shutdownHookThread = new ShutdownHookThread(logger, callback);

        shutdownHookThread.run();

        assertThat(callbackTimes.get()).isEqualTo(1);
        verify(logger).error(eq("shutdown hook callback invoked failure."), any(IllegalStateException.class));
    }
}
