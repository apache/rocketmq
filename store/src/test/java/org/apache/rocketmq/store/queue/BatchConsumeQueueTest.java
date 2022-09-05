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

package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreTestBase;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.lang.String.format;

public class BatchConsumeQueueTest extends StoreTestBase {

    List<BatchConsumeQueue> batchConsumeQueues = new ArrayList<>();

    private BatchConsumeQueue createBatchConsume(String path) {
        if (path == null) {
            path = createBaseDir();
        }
        baseDirs.add(path);
        MessageStore messageStore = null;
        try {
            messageStore = createMessageStore(null);
        } catch (Exception e) {
            Assert.fail();
        }
        BatchConsumeQueue batchConsumeQueue = new BatchConsumeQueue("topic", 0, path, fileSize, messageStore);
        batchConsumeQueues.add(batchConsumeQueue);
        return batchConsumeQueue;
    }

    private int fileSize = BatchConsumeQueue.CQ_STORE_UNIT_SIZE * 20;

    @Test(timeout = 2000)
    public void testBuildAndIterateBatchConsumeQueue() {
        BatchConsumeQueue batchConsumeQueue = createBatchConsume(null);
        batchConsumeQueue.load();
        short batchNum = 10;
        int unitNum = 10000;
        int initialMsgOffset = 1000;
        for (int i = 0; i < unitNum; i++) {
            batchConsumeQueue.putBatchMessagePositionInfo(i, 1024, 111, i * batchNum, i * batchNum + initialMsgOffset, batchNum);
        }
        Assert.assertEquals(500, getBcqFileSize(batchConsumeQueue));
        Assert.assertEquals(initialMsgOffset + batchNum * unitNum, batchConsumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(initialMsgOffset, batchConsumeQueue.getMinOffsetInQueue());

        {
            CqUnit first = batchConsumeQueue.getEarliestUnit();
            Assert.assertNotNull(first);
            Assert.assertEquals(initialMsgOffset, first.getQueueOffset());
            Assert.assertEquals(batchNum, first.getBatchNum());
        }

        {
            CqUnit last = batchConsumeQueue.getLatestUnit();
            Assert.assertNotNull(last);
            Assert.assertEquals(initialMsgOffset + batchNum * unitNum - batchNum, last.getQueueOffset());
            Assert.assertEquals(batchNum, last.getBatchNum());
        }

        for (int i = 0; i < initialMsgOffset + batchNum * unitNum + 10; i++) {
            ReferredIterator<CqUnit> it = batchConsumeQueue.iterateFrom(i);
            if (i < initialMsgOffset || i >= initialMsgOffset + batchNum * unitNum) {
                Assert.assertNull(it);
                continue;
            }
            Assert.assertNotNull(it);
            CqUnit cqUnit = it.nextAndRelease();
            Assert.assertNotNull(cqUnit);

            long baseOffset = (i / batchNum) * batchNum;
            Assert.assertEquals(baseOffset, cqUnit.getQueueOffset());
            Assert.assertEquals(batchNum, cqUnit.getBatchNum());

            Assert.assertEquals((i - initialMsgOffset) / batchNum, cqUnit.getPos());
            Assert.assertEquals(1024, cqUnit.getSize());
            Assert.assertEquals(111, cqUnit.getTagsCode());
            Assert.assertNull(cqUnit.getCqExtUnit());
        }
        batchConsumeQueue.destroy();
    }

    @Test(timeout = 10000)
    public void testBuildAndSearchBatchConsumeQueue() {
        // Preparing the data may take some time
        BatchConsumeQueue batchConsumeQueue = createBatchConsume(null);
        batchConsumeQueue.load();
        short batchSize = 10;
        int unitNum = 20000;
        for (int i = 0; i < unitNum; i++) {
            batchConsumeQueue.putBatchMessagePositionInfo(i, 100, 0, i * batchSize, i * batchSize + 1, batchSize);
        }
        Assert.assertEquals(1000, getBcqFileSize(batchConsumeQueue));
        Assert.assertEquals(unitNum * batchSize + 1, batchConsumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(1, batchConsumeQueue.getMinOffsetInQueue());
        batchConsumeQueue.reviseMaxAndMinOffsetInQueue();
        Assert.assertEquals(unitNum * batchSize + 1, batchConsumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(1, batchConsumeQueue.getMinOffsetInQueue());

        // test search the offset
        // lower bounds
        Assert.assertFalse(ableToFindResult(batchConsumeQueue, 0));
        Assert.assertTrue(ableToFindResult(batchConsumeQueue, 1));
        // upper bounds
        Assert.assertFalse(ableToFindResult(batchConsumeQueue, unitNum * batchSize + 1));
        Assert.assertTrue(ableToFindResult(batchConsumeQueue, unitNum * batchSize));
        // iterate every possible batch-msg offset
        for (int i = 1; i <= unitNum * batchSize; i++) {
            int expectedValue = ((i - 1) / batchSize) * batchSize + 1;
            SelectMappedBufferResult batchMsgIndexBuffer = batchConsumeQueue.getBatchMsgIndexBuffer(i);
            Assert.assertEquals(expectedValue, batchMsgIndexBuffer.getByteBuffer().getLong(BatchConsumeQueue.MSG_BASE_OFFSET_INDEX));
            batchMsgIndexBuffer.release();
        }
        SelectMappedBufferResult sbr = batchConsumeQueue.getBatchMsgIndexBuffer(501);
        Assert.assertEquals(501, sbr.getByteBuffer().getLong(BatchConsumeQueue.MSG_BASE_OFFSET_INDEX));
        Assert.assertEquals(10, sbr.getByteBuffer().getShort(BatchConsumeQueue.MSG_BASE_OFFSET_INDEX + 8));
        sbr.release();

        // test search the storeTime
        Assert.assertEquals(1, batchConsumeQueue.getOffsetInQueueByTime(-100));
        Assert.assertEquals(1, batchConsumeQueue.getOffsetInQueueByTime(0));
        Assert.assertEquals(11, batchConsumeQueue.getOffsetInQueueByTime(1));
        for (int i = 0; i < unitNum; i++) {
            int storeTime = i * batchSize;
            int expectedOffset = storeTime + 1;
            long offset = batchConsumeQueue.getOffsetInQueueByTime(storeTime);
            Assert.assertEquals(expectedOffset, offset);
        }
        Assert.assertEquals(199991, batchConsumeQueue.getOffsetInQueueByTime(System.currentTimeMillis()));
        batchConsumeQueue.destroy();
    }

    @Test(timeout = 2000)
    public void testBuildAndRecoverBatchConsumeQueue() {
        String tmpPath = createBaseDir();
        short batchSize = 10;
        {
            BatchConsumeQueue batchConsumeQueue = createBatchConsume(tmpPath);
            batchConsumeQueue.load();
            for (int i = 0; i < 100; i++) {
                batchConsumeQueue.putBatchMessagePositionInfo(i, 100, 0, i * batchSize, i * batchSize + 1, batchSize);
            }
            Assert.assertEquals(5, getBcqFileSize(batchConsumeQueue));
            Assert.assertEquals(1001, batchConsumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(1, batchConsumeQueue.getMinOffsetInQueue());

            for (int i = 0; i < 10; i++) {
                batchConsumeQueue.flush(0);
            }
        }
        {
            BatchConsumeQueue recover = createBatchConsume(tmpPath);
            recover.load();
            recover.recover();
            Assert.assertEquals(5, getBcqFileSize(recover));
            Assert.assertEquals(1001, recover.getMaxOffsetInQueue());
            Assert.assertEquals(1, recover.getMinOffsetInQueue());
            for (int i = 1; i <= 1000; i++) {
                int expectedValue = ((i - 1) / batchSize) * batchSize + 1;
                SelectMappedBufferResult batchMsgIndexBuffer = recover.getBatchMsgIndexBuffer(i);
                Assert.assertEquals(expectedValue, batchMsgIndexBuffer.getByteBuffer().getLong(BatchConsumeQueue.MSG_BASE_OFFSET_INDEX));
                batchMsgIndexBuffer.release();
            }
        }
    }

    @Test(timeout = 2000)
    public void testTruncateBatchConsumeQueue() {
        String tmpPath = createBaseDir();
        BatchConsumeQueue batchConsumeQueue = createBatchConsume(tmpPath);
        batchConsumeQueue.load();
        short batchSize = 10;
        int unitNum = 20000;
        for (int i = 0; i < unitNum; i++) {
            batchConsumeQueue.putBatchMessagePositionInfo(i, 100, 0, i * batchSize, i * batchSize + 1, batchSize);
        }
        Assert.assertEquals(1000, getBcqFileSize(batchConsumeQueue));
        Assert.assertEquals(unitNum * batchSize + 1, batchConsumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(1, batchConsumeQueue.getMinOffsetInQueue());

        int truncatePhyOffset = new Random().nextInt(unitNum);
        batchConsumeQueue.truncateDirtyLogicFiles(truncatePhyOffset);

        for (int i = 1; i < unitNum; i++) {
            long msgOffset = i * batchSize + 1;
            if (i < truncatePhyOffset) {
                int expectedValue = ((i - 1) / batchSize) * batchSize + 1;
                SelectMappedBufferResult batchMsgIndexBuffer = batchConsumeQueue.getBatchMsgIndexBuffer(i);
                Assert.assertEquals(expectedValue, batchMsgIndexBuffer.getByteBuffer().getLong(BatchConsumeQueue.MSG_BASE_OFFSET_INDEX));
                batchMsgIndexBuffer.release();
            } else {
                Assert.assertNull(format("i: %d, truncatePhyOffset: %d", i, truncatePhyOffset), batchConsumeQueue.getBatchMsgIndexBuffer(msgOffset));
            }
        }
    }

    @Test
    public void testTruncateAndDeleteBatchConsumeQueue() {
        String tmpPath = createBaseDir();
        BatchConsumeQueue batchConsumeQueue = createBatchConsume(tmpPath);
        batchConsumeQueue.load();
        short batchSize = 10;
        for (int i = 0; i < 100; i++) {
            batchConsumeQueue.putBatchMessagePositionInfo(i, 100, 0, i * batchSize, i * batchSize + 1, batchSize);
        }
        Assert.assertEquals(5, batchConsumeQueue.mappedFileQueue.getMappedFiles().size());
        Assert.assertEquals(1001, batchConsumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(1, batchConsumeQueue.getMinOffsetInQueue());

        batchConsumeQueue.truncateDirtyLogicFiles(80);

        Assert.assertEquals(4, batchConsumeQueue.mappedFileQueue.getMappedFiles().size());
        Assert.assertEquals(801, batchConsumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(1, batchConsumeQueue.getMinOffsetInQueue());

        //test
        batchConsumeQueue.deleteExpiredFile(30);
        Assert.assertEquals(3, batchConsumeQueue.mappedFileQueue.getMappedFiles().size());
        Assert.assertEquals(801, batchConsumeQueue.getMaxOffsetInQueue());
        Assert.assertEquals(301, batchConsumeQueue.getMinOffsetInQueue());

    }

    @After
    @Override
    public void clear() {
        super.clear();
        for (BatchConsumeQueue batchConsumeQueue : batchConsumeQueues) {
            batchConsumeQueue.destroy();
        }
    }

    private int getBcqFileSize(BatchConsumeQueue batchConsumeQueue) {
        return batchConsumeQueue.mappedFileQueue.getMappedFiles().size();
    }

    private boolean ableToFindResult(BatchConsumeQueue batchConsumeQueue, long msgOffset) {
        SelectMappedBufferResult batchMsgIndexBuffer = batchConsumeQueue.getBatchMsgIndexBuffer(msgOffset);
        try {
            return batchMsgIndexBuffer != null;
        } finally {
            if (batchMsgIndexBuffer != null) {
                batchMsgIndexBuffer.release();
            }
        }
    }

    protected MessageStore createMessageStore(String baseDir) throws Exception {
        if (baseDir == null) {
            baseDir = createBaseDir();
        }
        baseDirs.add(baseDir);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(100 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        messageStoreConfig.setMapperFileSizeBatchConsumeQueue(20 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(1024);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setEnableConsumeQueueExt(false);
        messageStoreConfig.setStorePathRootDir(baseDir);
        messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        messageStoreConfig.setHaListenPort(nextPort());
        messageStoreConfig.setMaxTransferBytesOnMessageInDisk(1024 * 1024);
        messageStoreConfig.setMaxTransferBytesOnMessageInMemory(1024 * 1024);
        messageStoreConfig.setMaxTransferCountOnMessageInDisk(1024);
        messageStoreConfig.setMaxTransferCountOnMessageInMemory(1024);
        messageStoreConfig.setSearchBcqByCacheEnable(true);

        return new DefaultMessageStore(
            messageStoreConfig,
            new BrokerStatsManager("simpleTest", true),
            (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {
            },
            new BrokerConfig());
    }

}
