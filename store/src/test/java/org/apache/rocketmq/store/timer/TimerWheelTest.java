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

import java.io.IOException;

import static org.junit.Assert.assertEquals;

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

    @After
    public void shutdown() {
        if (null != timerWheel) {
            timerWheel.shutdown();
        }
        if (null != baseDir) {
            StoreTestUtils.deleteFile(baseDir);
        }
    }


}
