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
package org.apache.rocketmq.store.timer;

import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimerWheelDirtyPageTest {
    private static final int PRECISION_MS = 1000;
    private String fileName;
    private TimerWheel wheel;

    @Before
    public void setUp() throws Exception {
        fileName = StoreTestUtils.createBaseDir();
        // 258 slots cover two full 4 KiB pages and a partial final page.
        wheel = new TimerWheel(fileName, 129, PRECISION_MS);
        wheel.flush();
    }

    @After
    public void tearDown() {
        if (wheel != null) {
            wheel.shutdown();
        }
        StoreTestUtils.deleteFile(fileName);
        StoreTestUtils.deleteFile(fileName + ".17");
        StoreTestUtils.deleteFile(fileName + ".18");
    }

    @Test
    public void testWritesAcrossPagesAndPartialLastPage() throws Exception {
        for (int index : new int[] {0, 127, 128, 255, 256, 257}) {
            wheel.putSlot(index * (long) PRECISION_MS, index + 1, index + 2, index + 3, index + 4);
            wheel.flush();
            assertFileMatchesBuffer();
        }
        wheel.flush();
        assertFileMatchesBuffer();
        TimerWheel reopened = new TimerWheel(fileName, 129, PRECISION_MS);
        try {
            for (int index : new int[] {0, 127, 128, 255, 256, 257}) {
                Slot slot = reopened.getSlot(index * (long) PRECISION_MS);
                assertEquals(index + 1, slot.firstPos);
                assertEquals(index + 2, slot.lastPos);
                assertEquals(index + 3, slot.num);
                assertEquals(index + 4, slot.magic);
            }
        } finally {
            reopened.shutdown();
        }
    }

    @Test
    public void testAllMutatorsAfterPageHasBeenFlushed() throws Exception {
        long time = 128L * PRECISION_MS;
        wheel.putSlot(time, 10, 20, 3, 4);
        wheel.flush();
        assertFileMatchesBuffer();

        wheel.putSlot(time, 30, 40);
        wheel.flush();
        assertFileMatchesBuffer();
        assertEquals(3, wheel.getSlot(time).num);
        assertEquals(4, wheel.getSlot(time).magic);

        wheel.reviseSlot(time, TimerWheel.IGNORE, 50, false);
        wheel.flush();
        assertFileMatchesBuffer();
        wheel.reviseSlot(time, 60, TimerWheel.IGNORE, false);
        wheel.flush();
        assertFileMatchesBuffer();

        long nextCycle = time + 258L * PRECISION_MS;
        wheel.reviseSlot(nextCycle, 70, 80, false);
        wheel.flush();
        assertEquals(60, wheel.getSlot(time).firstPos);
        assertEquals(50, wheel.getSlot(time).lastPos);
        assertFileMatchesBuffer();

        wheel.reviseSlot(nextCycle, TimerWheel.IGNORE, 90, true);
        wheel.flush();
        assertFileMatchesBuffer();
        assertEquals(90, wheel.getSlot(nextCycle).firstPos);
        assertEquals(90, wheel.getSlot(nextCycle).lastPos);
    }

    @Test
    public void testFailedCopyIsRetried() throws Exception {
        wheel.putSlot(0, 10, 20, 3, 4);
        wheel.putSlot(257L * PRECISION_MS, 30, 40, 5, 6);
        Field field = field("mappedByteBuffer");
        MappedByteBuffer original = (MappedByteBuffer) field.get(wheel);
        field.set(wheel, original.asReadOnlyBuffer());
        try {
            wheel.flush();
            fail("Expected the mapped write to fail");
        } catch (ReadOnlyBufferException expected) {
            // A failed copy must leave this page, and unvisited pages, eligible for retry.
        } finally {
            field.set(wheel, original);
        }
        wheel.flush();
        assertFileMatchesBuffer();
    }

    @Test
    public void testPartialPutIsFlushedAfterFailure() throws Exception {
        wheel.putSlot(0, 10, 20, 3, 4);
        wheel.flush();
        ByteBuffer buffer = localBuffer();
        buffer.limit(16);
        try {
            wheel.putSlot(0, 30, 40);
            fail("Expected the final long write to fail");
        } catch (BufferOverflowException expected) {
            // Timestamp and firstPos were already written before lastPos failed.
        } finally {
            buffer.limit(buffer.capacity());
        }
        wheel.flush();
        assertFileMatchesBuffer();
        assertEquals(30, wheel.getSlot(0).firstPos);
        assertEquals(20, wheel.getSlot(0).lastPos);
    }

    @Test
    public void testPartialFullSlotPutIsFlushedAfterFailure() throws Exception {
        wheel.putSlot(0, 10, 20, 3, 4);
        wheel.flush();
        ByteBuffer buffer = localBuffer();
        buffer.limit(28);
        try {
            wheel.putSlot(0, 30, 40, 5, 6);
            fail("Expected the magic write to fail");
        } catch (BufferOverflowException expected) {
            // num changed even though the last field could not be written.
        } finally {
            buffer.limit(buffer.capacity());
        }
        wheel.flush();
        assertFileMatchesBuffer();
        assertEquals(5, wheel.getSlot(0).num);
        assertEquals(4, wheel.getSlot(0).magic);
    }

    @Test
    public void testPartialReviseIsFlushedAfterFailure() throws Exception {
        wheel.putSlot(0, 10, 20, 3, 4);
        wheel.flush();
        ByteBuffer buffer = localBuffer();
        buffer.limit(16);
        try {
            wheel.reviseSlot(0, 30, 40, false);
            fail("Expected the lastPos update to fail");
        } catch (BufferOverflowException expected) {
            // Preserve the original partial-write behavior and flush the changed firstPos.
        } finally {
            buffer.limit(buffer.capacity());
        }
        wheel.flush();
        assertFileMatchesBuffer();
        assertEquals(30, wheel.getSlot(0).firstPos);
        assertEquals(20, wheel.getSlot(0).lastPos);
    }

    @Test(timeout = 30000)
    public void testConcurrentWritesAndFlushEventuallyPersistAllBytes() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(4);
        List<Future<?>> writers = new ArrayList<>();
        try {
            for (int writer = 0; writer < 4; writer++) {
                final int id = writer;
                writers.add(executor.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < 10000 && !Thread.currentThread().isInterrupted(); i++) {
                            // Distinct slots share pages, including both sides of a page boundary.
                            long time = (id * 64L + i % 64) * PRECISION_MS;
                            wheel.putSlot(time, i, i + 1, i + 2, id);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    } finally {
                        finished.countDown();
                    }
                }));
            }
            start.countDown();
            do {
                wheel.flush();
            } while (finished.getCount() != 0);
            for (Future<?> writer : writers) {
                writer.get(5, TimeUnit.SECONDS);
            }
            wheel.flush();
            assertFileMatchesBuffer();
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSnapshotBackupRemainsIndependentOfDirtyPages() throws Exception {
        wheel.putSlot(0, 10, 20, 3, 4);
        wheel.backup(17);
        TimerWheel snapshot = new TimerWheel(fileName, 129, PRECISION_MS, 17);
        try {
            assertEquals(10, snapshot.getSlot(0).firstPos);
            snapshot.putSlot(0, 30, 40, 5, 6);
            snapshot.flush(); // No mapped file exists in snapshot mode.
            snapshot.backup(18);
            TimerWheel recovered = new TimerWheel(fileName, 129, PRECISION_MS, 18);
            try {
                assertEquals(30, recovered.getSlot(0).firstPos);
                assertEquals(40, recovered.getSlot(0).lastPos);
                assertEquals(5, recovered.getSlot(0).num);
                assertEquals(6, recovered.getSlot(0).magic);
            } finally {
                recovered.shutdown(false);
            }
            assertEquals(10, wheel.getSlot(0).firstPos);
        } finally {
            snapshot.shutdown(false);
        }
    }

    private void assertFileMatchesBuffer() throws Exception {
        ByteBuffer buffer = ((ByteBuffer) field("byteBuffer").get(wheel)).duplicate();
        buffer.clear();
        byte[] expected = new byte[buffer.remaining()];
        buffer.get(expected);
        assertArrayEquals(expected, Files.readAllBytes(Paths.get(fileName)));
    }

    @SuppressWarnings("unchecked")
    private ByteBuffer localBuffer() throws Exception {
        return ((ThreadLocal<ByteBuffer>) field("localBuffer").get(wheel)).get();
    }

    private Field field(String name) throws Exception {
        Field field = TimerWheel.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
