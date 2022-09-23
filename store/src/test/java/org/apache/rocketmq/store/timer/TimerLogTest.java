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

import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.After;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TimerLogTest {

    private final Set<String> baseDirs = new HashSet<>();
    private final List<TimerLog> timerLogs = new ArrayList<>();

    public TimerLog createTimerLog(String baseDir) {
        if (null == baseDir) {
            baseDir = StoreTestUtils.createBaseDir();
        }
        TimerLog timerLog = new TimerLog(baseDir, 1024);
        timerLogs.add(timerLog);
        baseDirs.add(baseDir);
        timerLog.load();
        return timerLog;
    }

    @Test
    public void testAppendRollSelectDelete() throws Exception {
        TimerLog timerLog = createTimerLog(null);
        ByteBuffer byteBuffer = ByteBuffer.allocate(TimerLog.UNIT_SIZE);
        byteBuffer.putInt(TimerLog.UNIT_SIZE);
        byteBuffer.putLong(Long.MAX_VALUE);
        byteBuffer.putInt(0);
        byteBuffer.putLong(Long.MAX_VALUE);
        byteBuffer.putInt(0);
        byteBuffer.putLong(1000);
        byteBuffer.putInt(10);
        byteBuffer.putInt(123);
        byteBuffer.putInt(0);
        long ret = -1;
        for (int i = 0; i < 10; i++) {
            ret = timerLog.append(byteBuffer.array(), 0, TimerLog.UNIT_SIZE);
            assertEquals(i * TimerLog.UNIT_SIZE, ret);
        }
        for (int i = 0; i < 100; i++) {
            timerLog.append(byteBuffer.array());
        }
        assertEquals(6, timerLog.getMappedFileQueue().getMappedFiles().size());
        SelectMappedBufferResult sbr = timerLog.getTimerMessage(ret);
        assertNotNull(sbr);
        assertEquals(TimerLog.UNIT_SIZE, sbr.getByteBuffer().getInt());
        sbr.release();
        SelectMappedBufferResult wholeSbr = timerLog.getWholeBuffer(ret);
        assertEquals(0, wholeSbr.getStartOffset());
        wholeSbr.release();
        timerLog.getMappedFileQueue().deleteExpiredFileByOffsetForTimerLog(1024, timerLog.getOffsetForLastUnit(), TimerLog.UNIT_SIZE);
        assertEquals(1, timerLog.getMappedFileQueue().getMappedFiles().size());
    }

    @Test
    public void testRecovery() throws Exception {
        String basedir = StoreTestUtils.createBaseDir();
        TimerLog first = createTimerLog(basedir);
        first.append(new byte[512]);
        first.append(new byte[510]);
        byte[] data = "Hello Recovery".getBytes();
        first.append(data);
        first.shutdown();
        TimerLog second = createTimerLog(basedir);
        assertEquals(2, second.getMappedFileQueue().getMappedFiles().size());
        second.getMappedFileQueue().truncateDirtyFiles(1204 + 1000);
        SelectMappedBufferResult sbr = second.getTimerMessage(1024 + 510);
        byte[] expect = new byte[data.length];
        sbr.getByteBuffer().get(expect);
        assertArrayEquals(expect, data);
    }

    @After
    public void shutdown() {
        for (TimerLog timerLog : timerLogs) {
            timerLog.shutdown();
            timerLog.getMappedFileQueue().destroy();
        }
        for (String baseDir : baseDirs) {
            StoreTestUtils.deleteFile(baseDir);
        }
    }
}
