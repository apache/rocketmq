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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class InFlightRequestFutureTest {

    @Test
    public void testInFlightRequestFuture() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        futureList.add(Pair.of(32, CompletableFuture.completedFuture(1031L)));
        futureList.add(Pair.of(256, CompletableFuture.completedFuture(1287L)));
        InFlightRequestFuture future = new InFlightRequestFuture(1000, futureList);

        Assert.assertEquals(1000, future.getStartOffset());
        Assert.assertTrue(future.isFirstDone());
        Assert.assertTrue(future.isAllDone());
        Assert.assertEquals(1031, future.getFirstFuture().join().longValue());
        Assert.assertEquals(-1L, future.getFuture(0).join().longValue());
        Assert.assertEquals(1031L, future.getFuture(1024).join().longValue());
        Assert.assertEquals(1287L, future.getFuture(1200).join().longValue());
        Assert.assertEquals(-1L, future.getFuture(2000).join().longValue());
        Assert.assertEquals(1287L, future.getLastFuture().join().longValue());
        Assert.assertArrayEquals(futureList.stream().map(Pair::getRight).toArray(), future.getAllFuture().toArray());
    }

    @Test
    public void testInFlightRequestKey() {
        InFlightRequestKey requestKey1 = new InFlightRequestKey("group", 0, 0);
        InFlightRequestKey requestKey2 = new InFlightRequestKey("group", 1, 1);
        Assert.assertEquals(requestKey1, requestKey2);
        Assert.assertEquals(requestKey1.hashCode(), requestKey2.hashCode());
        Assert.assertEquals(requestKey1.getGroup(), requestKey2.getGroup());
        Assert.assertNotEquals(requestKey1.getOffset(), requestKey2.getOffset());
        Assert.assertNotEquals(requestKey1.getBatchSize(), requestKey2.getBatchSize());
    }

    @Test
    public void testGetStartOffset() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        long startOffset = inFlightRequestFuture.getStartOffset();
        Assert.assertEquals(10, startOffset);
    }

    @Test
    public void testGetFirstFuture() throws ExecutionException, InterruptedException {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        CompletableFuture<Long> firstFuture = inFlightRequestFuture.getFirstFuture();
        Assert.assertEquals(new Long(20), firstFuture.get());
    }

    @Test
    public void testGetFuture() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        CompletableFuture<Long> future = inFlightRequestFuture.getFuture(11);
        Assert.assertEquals(new Long(-1L), future.join());
    }

    @Test
    public void testGetLastFuture() throws ExecutionException, InterruptedException {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        CompletableFuture<Long> lastFuture = inFlightRequestFuture.getLastFuture();
        Assert.assertEquals(new Long(20), lastFuture.get());
    }

    @Test
    public void testIsFirstDone() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(20L);
        futureList.add(Pair.of(1, completableFuture));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        Assert.assertTrue(inFlightRequestFuture.isFirstDone());
    }

    @Test
    public void testIsAllDone() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture1 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture2 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture3 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture4 = new CompletableFuture<>();
        completableFuture1.complete(20L);
        completableFuture2.complete(30L);
        completableFuture3.complete(40L);
        futureList.add(Pair.of(1, completableFuture1));
        futureList.add(Pair.of(2, completableFuture2));
        futureList.add(Pair.of(3, completableFuture3));
        futureList.add(Pair.of(4, completableFuture4));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        Assert.assertFalse(inFlightRequestFuture.isAllDone());
    }

    @Test
    public void testGetAllFuture() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        CompletableFuture<Long> completableFuture1 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture2 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture3 = new CompletableFuture<>();
        CompletableFuture<Long> completableFuture4 = new CompletableFuture<>();
        futureList.add(Pair.of(1, completableFuture1));
        futureList.add(Pair.of(2, completableFuture2));
        futureList.add(Pair.of(3, completableFuture3));
        futureList.add(Pair.of(4, completableFuture4));
        InFlightRequestFuture inFlightRequestFuture = new InFlightRequestFuture(10, futureList);
        List<CompletableFuture<Long>> allFuture = inFlightRequestFuture.getAllFuture();
        Assert.assertEquals(4, allFuture.size());
    }
}
