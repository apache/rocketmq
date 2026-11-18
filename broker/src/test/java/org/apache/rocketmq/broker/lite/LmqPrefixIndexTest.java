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

package org.apache.rocketmq.broker.lite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.lite.LiteUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LmqPrefixIndexTest {

    private LmqPrefixIndex index;

    @Before
    public void setUp() {
        index = new LmqPrefixIndex();
    }

    // --- add ---

    @Test
    public void addBasic() {
        String lmqName = LiteUtil.toLmqName("topicA", "lite1");
        assertTrue(index.add(lmqName));
        assertEquals(1, index.size());
    }

    @Test
    public void addNull() {
        assertFalse(index.add(null));
        assertEquals(0, index.size());
    }

    @Test
    public void addDuplicate() {
        String lmqName = LiteUtil.toLmqName("topicA", "lite1");
        assertTrue(index.add(lmqName));
        assertFalse(index.add(lmqName));
        assertEquals(1, index.size());
    }

    // --- remove ---

    @Test
    public void removeExisting() {
        String lmqName = LiteUtil.toLmqName("topicA", "lite1");
        index.add(lmqName);
        assertTrue(index.remove(lmqName));
        assertEquals(0, index.size());
    }

    @Test
    public void removeNonExistent() {
        assertFalse(index.remove(LiteUtil.toLmqName("topicA", "nonexistent")));
        assertEquals(0, index.size());
    }

    // --- forEachLmqByPrefix ---

    @Test
    public void forEachByPrefixMatchesMultiple() {
        String lmq1 = LiteUtil.toLmqName("topicA", "lite1");
        String lmq2 = LiteUtil.toLmqName("topicA", "lite2");
        String lmq3 = LiteUtil.toLmqName("topicB", "lite1");
        index.add(lmq1);
        index.add(lmq2);
        index.add(lmq3);

        String prefix = LiteUtil.LITE_TOPIC_PREFIX + "topicA";
        List<String> collected = new ArrayList<>();
        boolean completed = index.forEachLmqByPrefix(prefix, name -> {
            collected.add(name);
            return true;
        });

        assertTrue(completed);
        assertEquals(2, collected.size());
        assertTrue(collected.contains(lmq1));
        assertTrue(collected.contains(lmq2));
        assertFalse(collected.contains(lmq3));
    }

    @Test
    public void forEachByPrefixExactMatch() {
        String lmq1 = LiteUtil.toLmqName("topicA", "lite1");
        String lmq2 = LiteUtil.toLmqName("topicA", "lite2");
        index.add(lmq1);
        index.add(lmq2);

        List<String> collected = new ArrayList<>();
        boolean completed = index.forEachLmqByPrefix(lmq1, name -> {
            collected.add(name);
            return true;
        });

        assertTrue(completed);
        assertEquals(1, collected.size());
        assertEquals(lmq1, collected.get(0));
    }

    @Test
    public void forEachByPrefixNoMatch() {
        index.add(LiteUtil.toLmqName("topicA", "lite1"));

        AtomicInteger visitCount = new AtomicInteger(0);
        boolean completed = index.forEachLmqByPrefix(
            LiteUtil.LITE_TOPIC_PREFIX + "topicX", name -> {
                visitCount.incrementAndGet();
                return true;
            });

        assertTrue(completed);
        assertEquals(0, visitCount.get());
    }

    @Test
    public void forEachByPrefixEarlyBreak() {
        index.add(LiteUtil.toLmqName("topicA", "lite1"));
        index.add(LiteUtil.toLmqName("topicA", "lite2"));
        index.add(LiteUtil.toLmqName("topicA", "lite3"));

        List<String> collected = new ArrayList<>();
        boolean completed = index.forEachLmqByPrefix(
            LiteUtil.LITE_TOPIC_PREFIX + "topicA", name -> {
                collected.add(name);
                return collected.size() < 2;
            });

        assertFalse(completed);
        assertEquals(2, collected.size());
    }

    @Test
    public void forEachByPrefixEmptyPrefix() {
        index.add(LiteUtil.toLmqName("topicA", "lite1"));

        AtomicInteger visitCount = new AtomicInteger(0);
        assertFalse(index.forEachLmqByPrefix("", name -> {
            visitCount.incrementAndGet();
            return true;
        }));
        assertFalse(index.forEachLmqByPrefix(null, name -> {
            visitCount.incrementAndGet();
            return true;
        }));
        assertEquals(0, visitCount.get());
    }

    @Test
    public void forEachByPrefixNullVisitor() {
        index.add(LiteUtil.toLmqName("topicA", "lite1"));
        assertFalse(index.forEachLmqByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicA", null));
    }

    // --- traversal safety: the visitor must never run under the index lock ---

    @Test
    public void forEachByPrefixVisitorMayMutateIndex() throws Exception {
        // Regression: the visitor used to run while the read lock was held, so a visitor that
        // transitively called remove()/add() tried to upgrade the read lock to the write lock and
        // blocked forever.
        String lmq1 = LiteUtil.toLmqName("topicA", "lite1");
        String lmq2 = LiteUtil.toLmqName("topicA", "lite2");
        index.add(lmq1);
        index.add(lmq2);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> future = executor.submit(() -> {
                return index.forEachLmqByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicA", name -> {
                    index.remove(name);
                    index.add(LiteUtil.toLmqName("topicB", "addedByVisitor"));
                    return true;
                });
            });
            assertTrue(future.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdown();
        }

        // both topicA entries were dropped by the visitor, the entry it added is the only survivor
        String addedByVisitor = LiteUtil.toLmqName("topicB", "addedByVisitor");
        assertEquals(1, index.size());
        assertEquals(Collections.singletonList(addedByVisitor),
            index.snapshotByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicB"));
    }

    @Test
    public void forEachByPrefixDoesNotHoldIndexLockDuringVisitor() throws Exception {
        // Regression: the write lock must stay free while the visitor runs, otherwise the store
        // reput thread blocks in onLmqCreate -> add() for the whole duration of the traversal.
        String lmq1 = LiteUtil.toLmqName("topicA", "lite1");
        String lmq2 = LiteUtil.toLmqName("topicA", "lite2");
        index.add(lmq1);

        CountDownLatch visitorEntered = new CountDownLatch(1);
        CountDownLatch releaseVisitor = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> traversal = executor.submit(() -> {
                return index.forEachLmqByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicA", name -> {
                    visitorEntered.countDown();
                    awaitQuietly(releaseVisitor);
                    return true;
                });
            });
            assertTrue(visitorEntered.await(5, TimeUnit.SECONDS));

            // the visitor is still parked inside the traversal, yet a concurrent add() completes
            Future<Boolean> writer = executor.submit(() -> {
                return index.add(lmq2);
            });
            assertTrue(writer.get(5, TimeUnit.SECONDS));
            assertFalse(traversal.isDone());

            releaseVisitor.countDown();
            assertTrue(traversal.get(5, TimeUnit.SECONDS));
        } finally {
            releaseVisitor.countDown();
            executor.shutdown();
        }

        assertEquals(2, index.size());
    }

    @Test
    public void forEachByPrefixTraversalUsesSnapshot() throws Exception {
        // The traversal keeps iterating the snapshot taken before the first callback: entries the
        // visitor removes are still visited exactly once, entries it adds are not visited at all.
        String lmq1 = LiteUtil.toLmqName("topicA", "lite1");
        String lmq2 = LiteUtil.toLmqName("topicA", "lite2");
        String lmq3 = LiteUtil.toLmqName("topicA", "lite3");
        index.add(lmq1);
        index.add(lmq2);

        List<String> visited = new ArrayList<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> future = executor.submit(() -> {
                return index.forEachLmqByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicA", name -> {
                    visited.add(name);
                    index.remove(name);
                    index.add(lmq3);
                    return true;
                });
            });
            assertTrue(future.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdown();
        }

        assertEquals(2, visited.size());
        assertTrue(visited.contains(lmq1));
        assertTrue(visited.contains(lmq2));
        assertFalse(visited.contains(lmq3));
        assertEquals(Collections.singletonList(lmq3),
            index.snapshotByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicA"));
    }

    // --- snapshotByPrefix ---

    @Test
    public void snapshotByPrefixReturnsIndependentCopy() {
        String lmq1 = LiteUtil.toLmqName("topicA", "lite1");
        String lmq2 = LiteUtil.toLmqName("topicA", "lite2");
        index.add(lmq1);
        index.add(lmq2);

        List<String> snapshot = index.snapshotByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicA");
        assertEquals(2, snapshot.size());

        // mutations after the snapshot must not leak into the copy already handed out
        index.remove(lmq1);
        index.add(LiteUtil.toLmqName("topicA", "lite3"));
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.contains(lmq1));
        assertTrue(snapshot.contains(lmq2));
    }

    @Test
    public void snapshotByPrefixInvalidPrefix() {
        index.add(LiteUtil.toLmqName("topicA", "lite1"));

        assertTrue(index.snapshotByPrefix("").isEmpty());
        assertTrue(index.snapshotByPrefix(null).isEmpty());
        assertEquals(1, index.size());
    }

    @Test
    public void snapshotByPrefixNoMatch() {
        index.add(LiteUtil.toLmqName("topicA", "lite1"));
        assertTrue(index.snapshotByPrefix(LiteUtil.LITE_TOPIC_PREFIX + "topicX").isEmpty());
    }

    // --- isEmpty / size ---

    @Test
    public void isEmptyAndSize() {
        assertTrue(index.isEmpty());
        assertEquals(0, index.size());

        String lmq1 = LiteUtil.toLmqName("topicA", "lite1");
        String lmq2 = LiteUtil.toLmqName("topicA", "lite2");

        index.add(lmq1);
        assertFalse(index.isEmpty());
        assertEquals(1, index.size());

        index.add(lmq2);
        assertEquals(2, index.size());

        index.remove(lmq1);
        assertFalse(index.isEmpty());
        assertEquals(1, index.size());

        index.remove(lmq2);
        assertTrue(index.isEmpty());
        assertEquals(0, index.size());
    }

    // --- concurrency ---

    @Test
    public void concurrentAddAndForEach() throws Exception {
        int threads = 4;
        int entriesPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < entriesPerThread; i++) {
                        String lmqName = LiteUtil.toLmqName("topic" + threadIdx, "lite" + i);
                        index.add(lmqName);
                    }
                    // concurrent prefix scan while other threads may still be writing
                    String prefix = LiteUtil.LITE_TOPIC_PREFIX + "topic" + threadIdx;
                    index.forEachLmqByPrefix(prefix, name -> true);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        assertEquals(threads * entriesPerThread, index.size());
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
