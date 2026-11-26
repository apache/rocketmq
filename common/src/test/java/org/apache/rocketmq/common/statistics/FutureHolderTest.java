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
package org.apache.rocketmq.common.statistics;

import java.util.concurrent.Future;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class FutureHolderTest {

    @Test
    public void testRemoveAllFutureCancelsAllFuturesWithoutInterrupting() {
        FutureHolder<String> holder = new FutureHolder<>();
        Future<?> first = mock(Future.class);
        Future<?> second = mock(Future.class);

        holder.addFuture("item", first);
        holder.addFuture("item", second);
        verifyNoInteractions(first, second);

        holder.removeAllFuture("item");

        verify(first).cancel(false);
        verify(second).cancel(false);
        verifyNoMoreInteractions(first, second);
    }

    @Test
    public void testRemoveAllFutureDoesNotAffectOtherKeys() {
        FutureHolder<String> holder = new FutureHolder<>();
        Future<?> first = mock(Future.class);
        Future<?> second = mock(Future.class);

        holder.addFuture("first", first);
        holder.addFuture("second", second);

        holder.removeAllFuture("first");

        verify(first).cancel(false);
        verifyNoInteractions(second);

        holder.removeAllFuture("second");

        verify(second).cancel(false);
        verifyNoMoreInteractions(first, second);
    }

    @Test
    public void testRemoveAllFutureIgnoresMissingAndRemovedKeys() {
        FutureHolder<String> holder = new FutureHolder<>();
        Future<?> future = mock(Future.class);

        holder.removeAllFuture("missing");

        holder.addFuture("item", future);
        holder.removeAllFuture("missing");
        verifyNoInteractions(future);

        holder.removeAllFuture("item");
        holder.removeAllFuture("item");

        verify(future).cancel(false);
        verifyNoMoreInteractions(future);
    }

    @Test
    public void testAddFutureAfterRemovalUsesNewLifecycle() {
        FutureHolder<String> holder = new FutureHolder<>();
        Future<?> previous = mock(Future.class);
        Future<?> current = mock(Future.class);

        holder.addFuture("item", previous);
        holder.removeAllFuture("item");

        holder.addFuture("item", current);
        verifyNoInteractions(current);
        holder.removeAllFuture("item");

        verify(previous).cancel(false);
        verify(current).cancel(false);
        verifyNoMoreInteractions(previous, current);
    }
}
