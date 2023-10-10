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
package org.apache.rocketmq.tieredstore.common;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;

public class InFlightRequestFuture {

    private final long startOffset;
    private final List<Pair<Integer, CompletableFuture<Long>>> futureList;

    public InFlightRequestFuture(long startOffset, @Nonnull List<Pair<Integer, CompletableFuture<Long>>> futureList) {
        this.startOffset = startOffset;
        this.futureList = futureList;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public CompletableFuture<Long> getFirstFuture() {
        return futureList.isEmpty() ? CompletableFuture.completedFuture(-1L) : futureList.get(0).getRight();
    }

    public CompletableFuture<Long> getFuture(long queueOffset) {
        if (queueOffset < startOffset) {
            return CompletableFuture.completedFuture(-1L);
        }
        long nextRequestOffset = startOffset;
        for (Pair<Integer, CompletableFuture<Long>> pair : futureList) {
            nextRequestOffset += pair.getLeft();
            if (queueOffset < nextRequestOffset) {
                return pair.getRight();
            }
        }
        return CompletableFuture.completedFuture(-1L);
    }

    public CompletableFuture<Long> getLastFuture() {
        return futureList.isEmpty() ?
            CompletableFuture.completedFuture(-1L) : futureList.get(futureList.size() - 1).getRight();
    }

    public boolean isFirstDone() {
        if (!futureList.isEmpty()) {
            return futureList.get(0).getRight().isDone();
        }
        return true;
    }

    public boolean isAllDone() {
        for (Pair<Integer, CompletableFuture<Long>> pair : futureList) {
            if (!pair.getRight().isDone()) {
                return false;
            }
        }
        return true;
    }

    public List<CompletableFuture<Long>> getAllFuture() {
        return futureList.stream().map(Pair::getValue).collect(Collectors.toList());
    }
}
