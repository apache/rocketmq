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
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class CommonTest {
    @Test
    public void testInflightRequestFuture() {
        List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
        futureList.add(Pair.of(32, CompletableFuture.completedFuture(1031L)));
        futureList.add(Pair.of(256, CompletableFuture.completedFuture(1287L)));
        InflightRequestFuture future = new InflightRequestFuture(1000, futureList);

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
    public void testInflightRequestKey() {
        InflightRequestKey requestKey1 = new InflightRequestKey("group", 0, 0);
        InflightRequestKey requestKey2 = new InflightRequestKey("group", 1, 1);
        Assert.assertEquals(requestKey1, requestKey2);
        Assert.assertEquals(requestKey1.hashCode(), requestKey2.hashCode());
        Assert.assertEquals(requestKey1.getGroup(), requestKey2.getGroup());
        Assert.assertNotEquals(requestKey1.getOffset(), requestKey2.getOffset());
        Assert.assertNotEquals(requestKey1.getBatchSize(), requestKey2.getBatchSize());
    }
}
