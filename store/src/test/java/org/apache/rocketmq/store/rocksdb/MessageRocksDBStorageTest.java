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

package org.apache.rocketmq.store.rocksdb;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.rocksdb.TimerRocksDBRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TIMELINE_CHECK_POINT;
import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TIMELINE_ROLL_CHECK_POINT;
import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TIMER_COLUMN_FAMILY;

public class MessageRocksDBStorageTest {

    /** Fixed delay time so window assertions never depend on the wall clock. */
    private static final long FIXED_DELAY_TIME_BASE = 2000000000000L;
    private static final long WINDOW = 3600000L;

    private MessageRocksDBStorage storage;
    private String storePath;

    @Before
    public void setUp() throws Exception {
        storePath = System.getProperty("java.io.tmpdir") + File.separator + "message_rocksdb_test_" + System.currentTimeMillis();
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(storePath);
        storage = new MessageRocksDBStorage(config);
    }

    @After
    public void tearDown() {
        if (null != storage) {
            storage.shutdown();
        }
        UtilAll.deleteFile(new File(storePath));
    }

    @Test
    public void testPutThenDelete() {
        long delayTime = System.currentTimeMillis() + 3600000L;
        String uniqKey = "0A0A0A0A00002A9F0000000000000003";

        TimerRocksDBRecord putRecord = new TimerRocksDBRecord(delayTime, uniqKey, 100L, 200, 0L, null);
        putRecord.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_PUT);

        List<TimerRocksDBRecord> putList = new ArrayList<>();
        putList.add(putRecord);
        storage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, putList);

        TimerRocksDBRecord deleteRecord = new TimerRocksDBRecord(delayTime, uniqKey, 100L, 200, 0L, null);
        deleteRecord.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_DELETE);

        List<TimerRocksDBRecord> deleteList = new ArrayList<>();
        deleteList.add(deleteRecord);
        storage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, deleteList);

        List<TimerRocksDBRecord> result = storage.scanRecordsForTimer(
            TIMER_COLUMN_FAMILY, delayTime - 1, delayTime + 1, 10, null);

        Assert.assertTrue(null == result || result.isEmpty());
    }

    @Test
    public void testPutThenUpdate() {
        long delayTime = System.currentTimeMillis() + 3600000L;
        String uniqKey = "0A0A0A0A00002A9F0000000000000004";

        TimerRocksDBRecord putRecord = new TimerRocksDBRecord(delayTime, uniqKey, 100L, 200, 0L, null);
        putRecord.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_PUT);

        List<TimerRocksDBRecord> putList = new ArrayList<>();
        putList.add(putRecord);
        storage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, putList);

        TimerRocksDBRecord updateRecord = new TimerRocksDBRecord(delayTime, uniqKey, 200L, 300, 1L, null);
        updateRecord.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_UPDATE);

        List<TimerRocksDBRecord> updateList = new ArrayList<>();
        updateList.add(updateRecord);
        storage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, updateList);

        List<TimerRocksDBRecord> result = storage.scanRecordsForTimer(
            TIMER_COLUMN_FAMILY, delayTime - 1, delayTime + 1, 10, null);

        Assert.assertNotNull("PUT then UPDATE should have 1 record", result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(200L, result.get(0).getOffsetPy());
        Assert.assertEquals(300, result.get(0).getSizePy());
    }

    @Test
    public void testDeleteThenUpdate() {
        long delayTime = System.currentTimeMillis() + 3600000L;
        String uniqKey = "0A0A0A0A00002A9F0000000000000001";

        TimerRocksDBRecord putRecord = new TimerRocksDBRecord(delayTime, uniqKey, 100L, 200, 0L, null);
        putRecord.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_PUT);

        List<TimerRocksDBRecord> putList = new ArrayList<>();
        putList.add(putRecord);
        storage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, putList);

        List<TimerRocksDBRecord> scanAfterPut = storage.scanRecordsForTimer(
            TIMER_COLUMN_FAMILY, delayTime - 1, delayTime + 1, 10, null);
        Assert.assertNotNull("PUT should create a record in RocksDB", scanAfterPut);
        Assert.assertEquals(1, scanAfterPut.size());

        TimerRocksDBRecord deleteRecord = new TimerRocksDBRecord(delayTime, uniqKey, 100L, 200, 0L, null);
        deleteRecord.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_DELETE);

        TimerRocksDBRecord updateRecord = new TimerRocksDBRecord(delayTime, uniqKey, 200L, 300, 1L, null);
        updateRecord.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_UPDATE);

        List<TimerRocksDBRecord> cudList = new ArrayList<>();
        cudList.add(deleteRecord);
        cudList.add(updateRecord);
        storage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, cudList);

        List<TimerRocksDBRecord> resultAfterDeleteUpdate = storage.scanRecordsForTimer(
            TIMER_COLUMN_FAMILY, delayTime - 1, delayTime + 1, 10, null);

        int recordCount = null == resultAfterDeleteUpdate ? 0 : resultAfterDeleteUpdate.size();
        Assert.assertEquals(0, recordCount);
    }

    @Test
    public void testWriteAndGetRollCheckpoint() {
        Assert.assertEquals(0L, storage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_ROLL_CHECK_POINT));

        long checkpoint = FIXED_DELAY_TIME_BASE;
        storage.writeCheckPointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_ROLL_CHECK_POINT, checkpoint);
        Assert.assertEquals(checkpoint, storage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_ROLL_CHECK_POINT));

        long nextCheckpoint = checkpoint + WINDOW;
        storage.writeCheckPointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_ROLL_CHECK_POINT, nextCheckpoint);
        Assert.assertEquals(nextCheckpoint, storage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_ROLL_CHECK_POINT));
    }

    @Test
    public void testRollCheckpointIsIndependentOfForwardCheckpoint() {
        storage.writeCheckPointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_CHECK_POINT, FIXED_DELAY_TIME_BASE);
        storage.writeCheckPointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_ROLL_CHECK_POINT, FIXED_DELAY_TIME_BASE + WINDOW);

        Assert.assertEquals(FIXED_DELAY_TIME_BASE,
            storage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_CHECK_POINT));
        Assert.assertEquals(FIXED_DELAY_TIME_BASE + WINDOW,
            storage.getCheckpointForTimer(TIMER_COLUMN_FAMILY, TIMELINE_ROLL_CHECK_POINT));
    }

    @Test
    public void testScanAdjacentWindowsNoOverlap() {
        long begin = FIXED_DELAY_TIME_BASE;

        writeTimerRecord(begin + 1, "roll-window-first", 11L, 111);
        writeTimerRecord(begin + WINDOW, "roll-window-boundary", 22L, 222);
        writeTimerRecord(begin + WINDOW + 1, "roll-window-second", 33L, 333);

        List<TimerRocksDBRecord> firstWindow = storage.scanRecordsForTimer(
            TIMER_COLUMN_FAMILY, begin, begin + WINDOW, 10, null);
        Assert.assertNotNull(firstWindow);
        Assert.assertEquals(1, firstWindow.size());
        Assert.assertEquals("roll-window-first", firstWindow.get(0).getUniqKey());

        List<TimerRocksDBRecord> secondWindow = storage.scanRecordsForTimer(
            TIMER_COLUMN_FAMILY, begin + WINDOW, begin + 2 * WINDOW, 10, null);
        Assert.assertNotNull(secondWindow);
        Assert.assertEquals(2, secondWindow.size());
        Assert.assertEquals("roll-window-boundary", secondWindow.get(0).getUniqKey());
        Assert.assertEquals("roll-window-second", secondWindow.get(1).getUniqKey());
    }

    private void writeTimerRecord(long delayTime, String uniqKey, long offsetPy, int sizePy) {
        TimerRocksDBRecord record = new TimerRocksDBRecord(delayTime, uniqKey, offsetPy, sizePy, 0L, null);
        record.setActionFlag(TimerRocksDBRecord.TIMER_ROCKSDB_PUT);
        List<TimerRocksDBRecord> list = new ArrayList<>();
        list.add(record);
        storage.writeRecordsForTimer(TIMER_COLUMN_FAMILY, list);
    }

}
