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

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ExceptionUtilsNestedTest {
    @Test
    public void testUnwrapMixedFutureWrappers() {
        Throwable cause = new IllegalArgumentException("invalid request");
        Throwable nested = new CompletionException(new ExecutionException(new CompletionException(cause)));
        assertSame(cause, ExceptionUtils.getRealException(nested));
    }

    @Test
    public void testKeepWrapperWithoutCause() {
        Throwable wrapper = new CompletionException(null);
        assertSame(wrapper, ExceptionUtils.getRealException(wrapper));
    }

    @Test
    public void testDoNotUnwrapDomainExceptions() {
        Throwable domain = new IllegalStateException("domain failure", new IllegalArgumentException());
        assertSame(domain, ExceptionUtils.getRealException(new ExecutionException(domain)));
        assertNull(ExceptionUtils.getRealException(null));
    }

    @Test(timeout = 1000)
    public void testCyclicFutureWrappersTerminate() {
        CompletionException first = new FutureWrapper("first");
        CompletionException second = new FutureWrapper("second");
        first.initCause(second);
        second.initCause(first);
        assertSame(first, ExceptionUtils.getRealException(first));
    }

    private static class FutureWrapper extends CompletionException {
        FutureWrapper(String message) {
            super(message);
        }
    }
}
