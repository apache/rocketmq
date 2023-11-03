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
package org.apache.rocketmq.tieredstore.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.tieredstore.TieredStoreTestUtil;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexStoreFileTest {

    private static final String TOPIC_NAME = "TopicTest";
    private static final int TOPIC_ID = 123;
    private static final int QUEUE_ID = 2;
    private static final long MESSAGE_OFFSET = 666L;
    private static final int MESSAGE_SIZE = 1024;
    private static final String KEY = "MessageKey";
    private static final Set<String> KEY_SET = Collections.singleton(KEY);

    private TieredMessageStoreConfig storeConfig;
    private IndexStoreFile indexStoreFile;

    @Before
    public void init() throws IOException {
        String filePath = Paths.get(System.getProperty("user.home"), "store_test", "index").toString();
        TieredStoreExecutor.init();
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setTieredStoreFilePath(filePath);
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(5);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(20);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
        indexStoreFile = new IndexStoreFile(storeConfig, System.currentTimeMillis());
    }

    @After
    public void shutdown() {
        if (this.indexStoreFile != null) {
            this.indexStoreFile.shutdown();
            this.indexStoreFile.destroy();
        }
        TieredStoreTestUtil.destroyMetadataStore();
        TieredStoreTestUtil.destroyTempDir(storeConfig.getTieredStoreFilePath());
        TieredStoreExecutor.shutdown();
    }

    @Test
    public void testIndexHeaderConstants() {
        Assert.assertEquals(0, IndexStoreFile.INDEX_MAGIC_CODE);
        Assert.assertEquals(4, IndexStoreFile.INDEX_BEGIN_TIME_STAMP);
        Assert.assertEquals(12, IndexStoreFile.INDEX_END_TIME_STAMP);
        Assert.assertEquals(20, IndexStoreFile.INDEX_SLOT_COUNT);
        Assert.assertEquals(24, IndexStoreFile.INDEX_ITEM_INDEX);
        Assert.assertEquals(28, IndexStoreFile.INDEX_HEADER_SIZE);
        Assert.assertEquals(0xCCDDEEFF ^ 1880681586 + 4, IndexStoreFile.BEGIN_MAGIC_CODE);
        Assert.assertEquals(0xCCDDEEFF ^ 1880681586 + 8, IndexStoreFile.END_MAGIC_CODE);
    }

    @Test
    public void basicMethodTest() throws IOException {
        long timestamp = System.currentTimeMillis();
        IndexStoreFile localFile = new IndexStoreFile(storeConfig, timestamp);
        Assert.assertEquals(timestamp, localFile.getTimestamp());

        // test file status
        Assert.assertEquals(IndexFile.IndexStatusEnum.UNSEALED, localFile.getFileStatus());
        localFile.doCompaction();
        Assert.assertEquals(IndexFile.IndexStatusEnum.SEALED, localFile.getFileStatus());

        // test hash
        Assert.assertEquals("TopicTest#MessageKey", localFile.buildKey(TOPIC_NAME, KEY));
        Assert.assertEquals(638347386, indexStoreFile.hashCode(localFile.buildKey(TOPIC_NAME, KEY)));

        // test calculate position
        long headerSize = IndexStoreFile.INDEX_HEADER_SIZE;
        Assert.assertEquals(headerSize + Long.BYTES * 2, indexStoreFile.getSlotPosition(2));
        Assert.assertEquals(headerSize + Long.BYTES * 5, indexStoreFile.getSlotPosition(5));
        Assert.assertEquals(headerSize + Long.BYTES * 5 + IndexItem.INDEX_ITEM_SIZE * 2,
            indexStoreFile.getItemPosition(2));
        Assert.assertEquals(headerSize + Long.BYTES * 5 + IndexItem.INDEX_ITEM_SIZE * 5,
            indexStoreFile.getItemPosition(5));
    }

    @Test
    public void basicPutGetTest() {
        long timestamp = indexStoreFile.getTimestamp();

        // check metadata
        Assert.assertEquals(timestamp, indexStoreFile.getTimestamp());
        Assert.assertEquals(0, indexStoreFile.getEndTimestamp());
        Assert.assertEquals(0, indexStoreFile.getIndexItemCount());
        Assert.assertEquals(0, indexStoreFile.getHashSlotCount());

        // not put success
        Assert.assertEquals(AppendResult.UNKNOWN_ERROR, indexStoreFile.putKey(
            null, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
        Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
            TOPIC_NAME, TOPIC_ID, QUEUE_ID, null, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
        Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
            TOPIC_NAME, TOPIC_ID, QUEUE_ID, Collections.emptySet(), MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));

        // first item is invalid
        for (int i = 0; i < storeConfig.getTieredStoreIndexFileMaxIndexNum() - 2; i++) {
            Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
                TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
            Assert.assertEquals(timestamp, indexStoreFile.getTimestamp());
            Assert.assertEquals(timestamp, indexStoreFile.getEndTimestamp());
            Assert.assertEquals(1, indexStoreFile.getHashSlotCount());
            Assert.assertEquals(i + 1, indexStoreFile.getIndexItemCount());
        }

        Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
            TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
        Assert.assertEquals(AppendResult.FILE_FULL, indexStoreFile.putKey(
            TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));

        Assert.assertEquals(timestamp, indexStoreFile.getTimestamp());
        Assert.assertEquals(timestamp, indexStoreFile.getEndTimestamp());
        Assert.assertEquals(1, indexStoreFile.getHashSlotCount());
        Assert.assertEquals(storeConfig.getTieredStoreIndexFileMaxIndexNum() - 1, indexStoreFile.getIndexItemCount());
    }

    @Test
    public void differentKeyPutTest() {
        long timestamp = indexStoreFile.getTimestamp();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 3; j++) {
                Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
                    TOPIC_NAME + i, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
            }
        }
        Assert.assertEquals(timestamp, indexStoreFile.getTimestamp());
        Assert.assertEquals(timestamp, indexStoreFile.getEndTimestamp());
        Assert.assertEquals(5, indexStoreFile.getHashSlotCount());
        Assert.assertEquals(5 * 3, indexStoreFile.getIndexItemCount());
    }

    @Test
    public void concurrentPutTest() throws InterruptedException {
        long timestamp = indexStoreFile.getTimestamp();

        ExecutorService executorService = Executors.newFixedThreadPool(
            4, new ThreadFactoryImpl("ConcurrentPutGetTest"));

        // first item is invalid
        int indexCount = storeConfig.getTieredStoreIndexFileMaxIndexNum() - 1;
        CountDownLatch latch = new CountDownLatch(indexCount);
        for (int i = 0; i < indexCount; i++) {
            executorService.submit(() -> {
                Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
                    TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
                latch.countDown();
            });
        }
        latch.await();

        executorService.shutdown();
        Assert.assertEquals(AppendResult.FILE_FULL, indexStoreFile.putKey(
            TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
        Assert.assertEquals(indexCount, indexStoreFile.getIndexItemCount());
    }

    @Test
    public void recoverFileTest() throws IOException {
        int indexCount = 10;
        long timestamp = indexStoreFile.getTimestamp();
        for (int i = 0; i < indexCount; i++) {
            Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
                TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
        }
        indexStoreFile.shutdown();
        Assert.assertEquals(indexCount, indexStoreFile.getIndexItemCount());
        indexStoreFile = new IndexStoreFile(storeConfig, timestamp);
        Assert.assertEquals(indexCount, indexStoreFile.getIndexItemCount());
    }

    @Test
    public void doCompactionTest() throws Exception {
        long timestamp = indexStoreFile.getTimestamp();
        indexStoreFile = new IndexStoreFile(storeConfig, System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(
                TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
        }

        ByteBuffer byteBuffer = indexStoreFile.doCompaction();
        TieredFileSegment fileSegment = new PosixFileSegment(
            storeConfig, FileSegmentType.INDEX, "store_index", 0L);
        fileSegment.append(byteBuffer, timestamp);
        fileSegment.commit();
        Assert.assertEquals(byteBuffer.limit(), fileSegment.getSize());
        fileSegment.destroyFile();
    }

    @Test
    public void queryAsyncFromUnsealedFileTest() throws Exception {
        long timestamp = indexStoreFile.getTimestamp();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 3; j++) {
                Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(TOPIC_NAME + i,
                    TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, System.currentTimeMillis()));
            }
        }
        List<IndexItem> itemList = indexStoreFile.queryAsync(
            TOPIC_NAME + "1", KEY, 64, timestamp, System.currentTimeMillis()).get();
        Assert.assertEquals(3, itemList.size());
    }

    @Test
    public void queryAsyncFromSegmentFileTest() throws ExecutionException, InterruptedException {
        long timestamp = indexStoreFile.getTimestamp();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 3; j++) {
                Assert.assertEquals(AppendResult.SUCCESS, indexStoreFile.putKey(TOPIC_NAME + i,
                    TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, System.currentTimeMillis()));
            }
        }

        ByteBuffer byteBuffer = indexStoreFile.doCompaction();
        TieredFileSegment fileSegment = new PosixFileSegment(
            storeConfig, FileSegmentType.INDEX, "store_index", 0L);
        fileSegment.append(byteBuffer, timestamp);
        fileSegment.commit();
        Assert.assertEquals(byteBuffer.limit(), fileSegment.getSize());
        indexStoreFile.destroy();

        indexStoreFile = new IndexStoreFile(storeConfig, fileSegment);

        // change topic
        List<IndexItem> itemList = indexStoreFile.queryAsync(
            TOPIC_NAME, KEY, 64, timestamp, System.currentTimeMillis()).get();
        Assert.assertEquals(0, itemList.size());

        // change key
        itemList = indexStoreFile.queryAsync(
            TOPIC_NAME, KEY + "1", 64, timestamp, System.currentTimeMillis()).get();
        Assert.assertEquals(0, itemList.size());

        itemList = indexStoreFile.queryAsync(
            TOPIC_NAME + "1", KEY, 64, timestamp, System.currentTimeMillis()).get();
        Assert.assertEquals(3, itemList.size());
    }
}