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

import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TIMER_COLUMN_FAMILY;

public class MessageRocksDBStorageTest {

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

}
