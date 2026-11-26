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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TimerWheelTest {

    private String baseDir;

    private final int slotsTotal = 30;
    private final int precisionMs = 500;
    private TimerWheel timerWheel;

    private final long defaultDelay = System.currentTimeMillis() / precisionMs * precisionMs;

    @Before
    public void init() throws IOException {
        baseDir = StoreTestUtils.createBaseDir();
        timerWheel = new TimerWheel(baseDir, slotsTotal, precisionMs);
    }

    @Test
    public void testPutGet() {
        long delayedTime = defaultDelay + precisionMs;

        Slot first = timerWheel.getSlot(delayedTime);
        assertEquals(-1, first.timeMs);
        assertEquals(-1, first.firstPos);
        assertEquals(-1, first.lastPos);

        timerWheel.putSlot(delayedTime, 1, 2, 3, 4);
        Slot second = timerWheel.getSlot(delayedTime);
        assertEquals(delayedTime, second.timeMs);
        assertEquals(1, second.firstPos);
        assertEquals(2, second.lastPos);
        assertEquals(3, second.num);
        assertEquals(4, second.magic);
    }

    @Test
    public void testGetNum() {
        long delayedTime = defaultDelay + precisionMs;

        timerWheel.putSlot(delayedTime, 1, 2, 3, 4);
        assertEquals(3, timerWheel.getNum(delayedTime));
        assertEquals(3, timerWheel.getAllNum(delayedTime));

        timerWheel.putSlot(delayedTime + 5 * precisionMs, 5, 6, 7, 8);
        assertEquals(7, timerWheel.getNum(delayedTime + 5 * precisionMs));
        assertEquals(10, timerWheel.getAllNum(delayedTime));
    }

    @Test
    public void testCheckPhyPos() {
        long delayedTime = defaultDelay + precisionMs;
        timerWheel.putSlot(delayedTime, 1, 100, 1, 0);
        timerWheel.putSlot(delayedTime + 5 * precisionMs, 2, 200, 2, 0);
        timerWheel.putSlot(delayedTime + 10 * precisionMs, 3, 300, 3, 0);

        assertEquals(1, timerWheel.checkPhyPos(delayedTime, 50));
        assertEquals(2, timerWheel.checkPhyPos(delayedTime, 100));
        assertEquals(3, timerWheel.checkPhyPos(delayedTime, 200));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime, 300));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime, 400));

        assertEquals(2, timerWheel.checkPhyPos(delayedTime + 5 * precisionMs, 50));
        assertEquals(2, timerWheel.checkPhyPos(delayedTime + 5 * precisionMs, 100));
        assertEquals(3, timerWheel.checkPhyPos(delayedTime + 5 * precisionMs, 200));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime + 5 * precisionMs, 300));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime + 5 * precisionMs, 400));
    }

    @Test
    public void testPutRevise() {
        long delayedTime = System.currentTimeMillis() / precisionMs * precisionMs + 3 * precisionMs;
        timerWheel.putSlot(delayedTime, 1, 2);

        timerWheel.reviseSlot(delayedTime + 5 * precisionMs, 3, 4, false);
        Slot second = timerWheel.getSlot(delayedTime);
        assertEquals(delayedTime, second.timeMs);
        assertEquals(1, second.firstPos);
        assertEquals(2, second.lastPos);

        timerWheel.reviseSlot(delayedTime, TimerWheel.IGNORE, 4, false);
        Slot three = timerWheel.getSlot(delayedTime);
        assertEquals(1, three.firstPos);
        assertEquals(4, three.lastPos);

        timerWheel.reviseSlot(delayedTime, 3, TimerWheel.IGNORE, false);
        Slot four = timerWheel.getSlot(delayedTime);
        assertEquals(3, four.firstPos);
        assertEquals(4, four.lastPos);

        timerWheel.reviseSlot(delayedTime + 2 * slotsTotal * precisionMs, TimerWheel.IGNORE, 5, true);
        Slot five = timerWheel.getRawSlot(delayedTime);
        assertEquals(delayedTime + 2 * slotsTotal * precisionMs, five.timeMs);
        assertEquals(5, five.firstPos);
        assertEquals(5, five.lastPos);
    }

    @Test
    public void testRecoveryData() throws Exception {
        long delayedTime = System.currentTimeMillis() / precisionMs * precisionMs + 5 * precisionMs;
        timerWheel.putSlot(delayedTime, 1, 2, 3, 4);
        timerWheel.flush();

        TimerWheel tmpWheel = new TimerWheel(baseDir, slotsTotal, precisionMs);
        Slot slot = tmpWheel.getSlot(delayedTime);
        assertEquals(delayedTime, slot.timeMs);
        assertEquals(1, slot.firstPos);
        assertEquals(2, slot.lastPos);
        assertEquals(3, slot.num);
        assertEquals(4, slot.magic);

        tmpWheel.shutdown();
    }

    @Test(expected = RuntimeException.class)
    public void testRecoveryFixedTTL() throws Exception {
        timerWheel.flush();
        TimerWheel tmpWheel = new TimerWheel(baseDir, slotsTotal + 1, precisionMs);
    }

    @Test
    public void copyForSnapshotIsolationTest() throws Exception {
        long delayedTime = defaultDelay + precisionMs;
        timerWheel.putSlot(delayedTime, 1, 2, 3, 4);
        timerWheel.copyForSnapshot();
        // writes after the freeze must not leak into the snapshot
        timerWheel.putSlot(delayedTime, 5, 6, 7, 8);
        timerWheel.putSlot(delayedTime + precisionMs, 9, 10, 11, 12);
        timerWheel.backup(12345);

        TimerWheel snapWheel = new TimerWheel(baseDir, slotsTotal, precisionMs, 12345);
        Slot slot = snapWheel.getSlot(delayedTime);
        assertEquals(1, slot.firstPos);
        assertEquals(2, slot.lastPos);
        assertEquals(3, slot.num);
        assertEquals(4, slot.magic);
        assertEquals(-1, snapWheel.getSlot(delayedTime + precisionMs).timeMs);
        snapWheel.shutdown();

        timerWheel.copyForSnapshot();
        timerWheel.backup(12345);
        TimerWheel replacement = new TimerWheel(baseDir, slotsTotal, precisionMs, 12345);
        try {
            assertEquals(5, replacement.getSlot(delayedTime).firstPos);
            assertEquals(6, replacement.getSlot(delayedTime).lastPos);
            assertEquals(7, replacement.getSlot(delayedTime).num);
            assertEquals(9, replacement.getSlot(delayedTime + precisionMs).firstPos);
        } finally {
            replacement.shutdown();
        }
    }

    @Test
    public void segmentedBackupRoundTripTest() throws Exception {
        int bigSlots = 40000;
        String bigFileName = baseDir + "_big";
        TimerWheel bigWheel = new TimerWheel(bigFileName, bigSlots, precisionMs);
        for (int i = 0; i < bigSlots; i++) {
            bigWheel.putSlot(defaultDelay + (long) i * precisionMs, i, i + 1, i + 2, i + 3);
        }
        bigWheel.copyForSnapshot();
        bigWheel.backup(67890);
        assertEquals((long) bigSlots * 2 * Slot.SIZE, new File(bigFileName + ".67890").length());
        TimerWheel snapWheel = new TimerWheel(bigFileName, bigSlots, precisionMs, 67890);
        for (int i = 0; i < bigSlots; i++) {
            Slot slot = snapWheel.getRawSlot(defaultDelay + (long) i * precisionMs);
            assertEquals(i, slot.firstPos);
            assertEquals(i + 1, slot.lastPos);
            assertEquals(i + 2, slot.num);
            assertEquals(i + 3, slot.magic);
        }
        snapWheel.shutdown();
        bigWheel.shutdown();
        StoreTestUtils.deleteFile(bigFileName);
        StoreTestUtils.deleteFile(bigFileName + ".67890");
    }

    @Test
    public void promoteToBaseRoundTripTest() throws Exception {
        // snapshot-mode wheel on a dedicated path holds no handle on base, so the atomic
        // replace also succeeds on Windows, where a mmap-held base cannot be replaced
        String dir = StoreTestUtils.createBaseDir();
        assertTrue(new File(dir).mkdirs());
        String wheelPath = dir + File.separator + TimerWheel.TIMER_WHEEL_FILE_NAME;
        TimerWheel wheel = new TimerWheel(wheelPath, slotsTotal, precisionMs, -1, true);
        long delayedTime = defaultDelay + precisionMs;
        wheel.putSlot(delayedTime, 1, 2, 3, 4);
        wheel.copyForSnapshot();
        wheel.backup(100);
        assertTrue(new File(wheelPath + ".100").exists());

        wheel.promoteToBase(100);
        assertFalse(new File(wheelPath + ".100").exists());
        assertFalse(new File(wheelPath + ".100.tmp").exists());

        // base now holds the snapshot content; read it via the non-snapshot (mmap) constructor
        TimerWheel baseWheel = new TimerWheel(wheelPath, slotsTotal, precisionMs);
        Slot slot = baseWheel.getSlot(delayedTime);
        assertEquals(1, slot.firstPos);
        assertEquals(2, slot.lastPos);
        assertEquals(3, slot.num);
        assertEquals(4, slot.magic);
        baseWheel.shutdown();
        wheel.shutdown();
        StoreTestUtils.deleteFile(dir);
    }

    @Test
    public void promoteToBaseReplacesOldBaseTest() throws Exception {
        String dir = StoreTestUtils.createBaseDir();
        assertTrue(new File(dir).mkdirs());
        String wheelPath = dir + File.separator + TimerWheel.TIMER_WHEEL_FILE_NAME;
        TimerWheel wheel = new TimerWheel(wheelPath, slotsTotal, precisionMs, -1, true);
        long delayedTime = defaultDelay + precisionMs;

        // first promote lays down an old base
        wheel.putSlot(delayedTime, 1, 1, 1, 1);
        wheel.copyForSnapshot();
        wheel.backup(100);
        wheel.promoteToBase(100);
        assertTrue(new File(wheelPath).exists());
        assertEquals((long) slotsTotal * 2 * Slot.SIZE, new File(wheelPath).length());

        // second promote must REPLACE the existing base, not skip or fail
        wheel.putSlot(delayedTime, 9, 9, 9, 9);
        wheel.copyForSnapshot();
        wheel.backup(200);
        wheel.promoteToBase(200);
        assertFalse(new File(wheelPath + ".200").exists());
        assertEquals((long) slotsTotal * 2 * Slot.SIZE, new File(wheelPath).length());

        TimerWheel baseWheel = new TimerWheel(wheelPath, slotsTotal, precisionMs);
        Slot slot = baseWheel.getSlot(delayedTime);
        assertEquals(9, slot.firstPos);
        assertEquals(9, slot.num);
        baseWheel.shutdown();
        wheel.shutdown();
        StoreTestUtils.deleteFile(dir);
    }

    @Test
    public void missingSnapshotDoesNotPromoteTest() throws IOException {
        long delayedTime = defaultDelay + precisionMs;
        timerWheel.putSlot(delayedTime, 1, 2, 3, 4);
        timerWheel.flush();
        File baseFile = new File(baseDir);
        File snapshotFile = new File(baseDir + ".100");
        assertTrue(baseFile.isFile());
        assertFalse(snapshotFile.exists());

        assertThrows(IOException.class, () -> timerWheel.promoteToBase(100));
        assertTrue(baseFile.isFile());
        assertFalse(snapshotFile.exists());
        assertEquals((long) slotsTotal * 2 * Slot.SIZE, baseFile.length());

        TimerWheel reopened = new TimerWheel(baseDir, slotsTotal, precisionMs);
        try {
            Slot slot = reopened.getSlot(delayedTime);
            assertEquals(delayedTime, slot.timeMs);
            assertEquals(1, slot.firstPos);
            assertEquals(2, slot.lastPos);
            assertEquals(3, slot.num);
            assertEquals(4, slot.magic);
        } finally {
            reopened.shutdown();
        }
    }

    @Test
    public void snapshotModeNoMmapTest() throws Exception {
        String path = baseDir + "_snap";
        // snapshot mode with no snapshot yet loads the base file but must not mmap it
        TimerWheel snapWheel = new TimerWheel(path, slotsTotal, precisionMs, -1, true);
        snapWheel.putSlot(defaultDelay + precisionMs, 1, 2, 3, 4);
        snapWheel.flush(); // no-op in snapshot mode

        TimerWheel baseReader = new TimerWheel(path, slotsTotal, precisionMs);
        assertEquals(-1, baseReader.getSlot(defaultDelay + precisionMs).timeMs);
        baseReader.shutdown();
        // the in-memory wheel still holds the slot
        assertEquals(3, snapWheel.getNum(defaultDelay + precisionMs));
        snapWheel.shutdown();
        StoreTestUtils.deleteFile(path);
    }

    @Test
    public void promoteCleansLegacySnapshotsTest() throws Exception {
        String dir = StoreTestUtils.createBaseDir();
        assertTrue(new File(dir).mkdirs());
        String wheelPath = dir + File.separator + TimerWheel.TIMER_WHEEL_FILE_NAME;
        TimerWheel wheel = new TimerWheel(wheelPath, slotsTotal, precisionMs, -1, true);
        wheel.putSlot(defaultDelay + precisionMs, 1, 2, 3, 4);
        wheel.copyForSnapshot();
        wheel.backup(100);
        wheel.backup(200);
        assertEquals(200, TimerWheel.getMaxSnapshotFlag(wheelPath));

        wheel.promoteToBase(200);
        assertEquals(-1, TimerWheel.getMaxSnapshotFlag(wheelPath));
        assertFalse(new File(wheelPath + ".100").exists());
        assertFalse(new File(wheelPath + ".200").exists());

        TimerWheel baseReader = new TimerWheel(wheelPath, slotsTotal, precisionMs);
        assertEquals(3, baseReader.getSlot(defaultDelay + precisionMs).num);
        baseReader.shutdown();
        wheel.shutdown();
        StoreTestUtils.deleteFile(dir);
    }

    @After
    public void shutdown() {
        if (null != timerWheel) {
            timerWheel.shutdown();
        }
        if (null != baseDir) {
            StoreTestUtils.deleteFile(baseDir);
            StoreTestUtils.deleteFile(baseDir + ".12345");
            StoreTestUtils.deleteFile(baseDir + ".12345.tmp");
        }
    }


}
