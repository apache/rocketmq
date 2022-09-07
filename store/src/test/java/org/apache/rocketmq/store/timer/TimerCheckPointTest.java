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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimerCheckPointTest {

    private String baseDir;

    @Before
    public void init() throws IOException {
        baseDir = StoreTestUtils.createBaseDir();
    }

    @Test
    public void testCheckPoint() throws IOException {
        String baseSrc = baseDir + File.separator + "timercheck";
        TimerCheckpoint first = new TimerCheckpoint(baseSrc);
        assertEquals(0, first.getLastReadTimeMs());
        assertEquals(0, first.getLastTimerLogFlushPos());
        assertEquals(0, first.getLastTimerQueueOffset());
        assertEquals(0, first.getMasterTimerQueueOffset());
        first.setLastReadTimeMs(1000);
        first.setLastTimerLogFlushPos(1100);
        first.setLastTimerQueueOffset(1200);
        first.setMasterTimerQueueOffset(1300);
        first.shutdown();
        TimerCheckpoint second = new TimerCheckpoint(baseSrc);
        assertEquals(1000, second.getLastReadTimeMs());
        assertEquals(1100, second.getLastTimerLogFlushPos());
        assertEquals(1200, second.getLastTimerQueueOffset());
        assertEquals(1300, second.getMasterTimerQueueOffset());
    }

    @Test
    public void testNewCheckPoint() throws IOException {
        String baseSrc = baseDir + File.separator + "timercheck2";
        TimerCheckpoint first = new TimerCheckpoint(baseSrc);
        assertEquals(0, first.getLastReadTimeMs());
        assertEquals(0, first.getLastTimerLogFlushPos());
        assertEquals(0, first.getLastTimerQueueOffset());
        assertEquals(0, first.getMasterTimerQueueOffset());
        assertEquals(0, first.getDataVersion().getStateVersion());
        assertEquals(0, first.getDataVersion().getCounter().get());
        first.setLastReadTimeMs(1000);
        first.setLastTimerLogFlushPos(1100);
        first.setLastTimerQueueOffset(1200);
        first.setMasterTimerQueueOffset(1300);
        first.getDataVersion().setStateVersion(1400);
        first.getDataVersion().setTimestamp(1500);
        first.getDataVersion().setCounter(new AtomicLong(1600));
        first.shutdown();
        TimerCheckpoint second = new TimerCheckpoint(baseSrc);
        assertEquals(1000, second.getLastReadTimeMs());
        assertEquals(1100, second.getLastTimerLogFlushPos());
        assertEquals(1200, second.getLastTimerQueueOffset());
        assertEquals(1300, second.getMasterTimerQueueOffset());
        assertEquals(1400, second.getDataVersion().getStateVersion());
        assertEquals(1500, second.getDataVersion().getTimestamp());
        assertEquals(1600, second.getDataVersion().getCounter().get());
    }

    @Test
    public void testEncodeDecode() throws IOException {
        TimerCheckpoint first = new TimerCheckpoint();
        first.setLastReadTimeMs(1000);
        first.setLastTimerLogFlushPos(1100);
        first.setLastTimerQueueOffset(1200);
        first.setMasterTimerQueueOffset(1300);

        TimerCheckpoint second = TimerCheckpoint.decode(TimerCheckpoint.encode(first));
        assertEquals(first.getLastReadTimeMs(), second.getLastReadTimeMs());
        assertEquals(first.getLastTimerLogFlushPos(), second.getLastTimerLogFlushPos());
        assertEquals(first.getLastTimerQueueOffset(), second.getLastTimerQueueOffset());
        assertEquals(first.getMasterTimerQueueOffset(), second.getMasterTimerQueueOffset());
    }

    @Test
    public void testNewEncodeDecode() throws IOException {
        TimerCheckpoint first = new TimerCheckpoint();
        first.setLastReadTimeMs(1000);
        first.setLastTimerLogFlushPos(1100);
        first.setLastTimerQueueOffset(1200);
        first.setMasterTimerQueueOffset(1300);
        first.getDataVersion().setStateVersion(1400);
        first.getDataVersion().setTimestamp(1500);
        first.getDataVersion().setCounter(new AtomicLong(1600));
        TimerCheckpoint second = TimerCheckpoint.decode(TimerCheckpoint.encode(first));
        assertEquals(first.getLastReadTimeMs(), second.getLastReadTimeMs());
        assertEquals(first.getLastTimerLogFlushPos(), second.getLastTimerLogFlushPos());
        assertEquals(first.getLastTimerQueueOffset(), second.getLastTimerQueueOffset());
        assertEquals(first.getMasterTimerQueueOffset(), second.getMasterTimerQueueOffset());
        assertEquals(first.getDataVersion().getStateVersion(), 1400);
        assertEquals(first.getDataVersion().getTimestamp(), 1500);
        assertEquals(first.getDataVersion().getCounter().get(), 1600);
    }

    @After
    public void shutdown() {
        if (null != baseDir) {
            StoreTestUtils.deleteFile(baseDir);
        }
    }
}
