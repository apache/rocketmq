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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.tieredstore.TieredMessageStore;

import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.file.FlatFileFactory;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.await;

public class IndexStoreServiceTest {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static final String TOPIC_NAME = "TopicTest";
    private static final int TOPIC_ID = 123;
    private static final int QUEUE_ID = 2;
    private static final long MESSAGE_OFFSET = 666;
    private static final int MESSAGE_SIZE = 1024;
    private static final Set<String> KEY_SET = Collections.singleton("MessageKey");

    private String filePath;
    private MessageStoreConfig storeConfig;
    private FlatFileFactory fileAllocator;
    private IndexStoreService indexService;
    private TieredMessageStore messageStore;

    @Before
    public void init() throws IOException, ClassNotFoundException, NoSuchMethodException {
        filePath = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String directory = Paths.get(System.getProperty("user.home"), "store_test", filePath).toString();
        storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(directory);
        storeConfig.setTieredStoreFilePath(directory);
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(5);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(20);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.PosixFileSegment");
        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        fileAllocator = new FlatFileFactory(metadataStore, storeConfig);

        messageStore = Mockito.mock(TieredMessageStore.class);
        DefaultMessageStore defaultMessageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(defaultMessageStore.getMessageStoreConfig()).thenReturn(new org.apache.rocketmq.store.config.MessageStoreConfig());
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultMessageStore);
    }

    @After
    public void shutdown() {
        if (indexService != null) {
            indexService.shutdown();
            indexService.destroy();
        }
        MessageStoreUtilTest.deleteStoreDirectory(storeConfig.getTieredStoreFilePath());
    }

    @Test
    public void basicServiceTest() throws InterruptedException {
        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        for (int i = 0; i < 50; i++) {
            Assert.assertEquals(AppendResult.SUCCESS, indexService.putKey(
                TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, i * 100, MESSAGE_SIZE, System.currentTimeMillis()));
            TimeUnit.MILLISECONDS.sleep(1);
        }
        ConcurrentSkipListMap<Long, IndexFile> timeStoreTable = indexService.getTimeStoreTable();
        Assert.assertEquals(3, timeStoreTable.size());
    }

    @Test
    public void doConvertOldFormatTest() throws IOException {
        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        long timestamp = indexService.getTimeStoreTable().firstKey();
        Assert.assertEquals(AppendResult.SUCCESS, indexService.putKey(
            TOPIC_NAME, TOPIC_ID, QUEUE_ID, KEY_SET, MESSAGE_OFFSET, MESSAGE_SIZE, timestamp));
        indexService.shutdown();

        File file = new File(Paths.get(filePath, IndexStoreService.FILE_DIRECTORY_NAME, String.valueOf(timestamp)).toString());
        DefaultMappedFile mappedFile = new DefaultMappedFile(file.getName(), (int) file.length());
        mappedFile.renameTo(String.valueOf(new File(file.getParent(), "0000")));
        mappedFile.shutdown(10 * 1000);

        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        ConcurrentSkipListMap<Long, IndexFile> timeStoreTable = indexService.getTimeStoreTable();
        Assert.assertEquals(1, timeStoreTable.size());
        Assert.assertEquals(Long.valueOf(timestamp), timeStoreTable.firstKey());
        mappedFile.destroy(10 * 1000);
    }

    @Test
    public void concurrentPutTest() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(
            4, new ThreadFactoryImpl("ConcurrentPutTest"));
        storeConfig.setTieredStoreIndexFileMaxHashSlotNum(500);
        storeConfig.setTieredStoreIndexFileMaxIndexNum(2000);
        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        long timestamp = System.currentTimeMillis();

        // first item is invalid
        AtomicInteger success = new AtomicInteger();
        int indexCount = 5000;
        CountDownLatch latch = new CountDownLatch(indexCount);
        for (int i = 0; i < indexCount; i++) {
            final int index = i;
            executorService.submit(() -> {
                try {
                    AppendResult result = indexService.putKey(
                        TOPIC_NAME, TOPIC_ID, QUEUE_ID, Collections.singleton(String.valueOf(index)),
                        index * 100, MESSAGE_SIZE, timestamp + index);
                    if (AppendResult.SUCCESS.equals(result)) {
                        success.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("ConcurrentPutTest error", e);
                } finally {
                    latch.countDown();
                }
            });
        }
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        Assert.assertEquals(3, indexService.getTimeStoreTable().size());
        executorService.shutdown();
    }

    @Test
    public void doCompactionTest() throws InterruptedException {
        concurrentPutTest();
        IndexFile indexFile = indexService.getNextSealedFile();
        Assert.assertEquals(IndexFile.IndexStatusEnum.SEALED, indexFile.getFileStatus());

        indexService.doCompactThenUploadFile(indexFile);
        indexService.setCompactTimestamp(indexFile.getTimestamp());
        indexFile.destroy();

        List<IndexFile> files = new ArrayList<>(indexService.getTimeStoreTable().values());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UPLOAD, files.get(0).getFileStatus());
        Assert.assertEquals(IndexFile.IndexStatusEnum.SEALED, files.get(1).getFileStatus());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UNSEALED, files.get(2).getFileStatus());

        indexFile = indexService.getNextSealedFile();
        indexService.doCompactThenUploadFile(indexFile);
        indexService.setCompactTimestamp(indexFile.getTimestamp());
        files = new ArrayList<>(indexService.getTimeStoreTable().values());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UPLOAD, files.get(0).getFileStatus());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UPLOAD, files.get(1).getFileStatus());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UNSEALED, files.get(2).getFileStatus());

        indexFile = indexService.getNextSealedFile();
        Assert.assertNull(indexFile);
        files = new ArrayList<>(indexService.getTimeStoreTable().values());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UPLOAD, files.get(0).getFileStatus());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UPLOAD, files.get(1).getFileStatus());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UNSEALED, files.get(2).getFileStatus());
    }

    @Test
    public void runServiceTest() throws InterruptedException {
        concurrentPutTest();
        indexService.start();
        await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            boolean result = true;
            ArrayList<IndexFile> files = new ArrayList<>(indexService.getTimeStoreTable().values());
            result &= IndexFile.IndexStatusEnum.UPLOAD.equals(files.get(0).getFileStatus());
            result &= IndexFile.IndexStatusEnum.UPLOAD.equals(files.get(1).getFileStatus());
            result &= IndexFile.IndexStatusEnum.UNSEALED.equals(files.get(2).getFileStatus());
            return result;
        });
    }

    @Test
    public void restartServiceTest() throws InterruptedException {
        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        for (int i = 0; i < 20; i++) {
            AppendResult result = indexService.putKey(
                TOPIC_NAME, TOPIC_ID, QUEUE_ID, Collections.singleton(String.valueOf(i)),
                i * 100L, MESSAGE_SIZE, System.currentTimeMillis());
            Assert.assertEquals(AppendResult.SUCCESS, result);
            TimeUnit.MILLISECONDS.sleep(1);
        }
        long timestamp = indexService.getTimeStoreTable().firstKey();
        indexService.shutdown();
        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        Assert.assertEquals(timestamp, indexService.getTimeStoreTable().firstKey().longValue());

        indexService.start();
        await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            ArrayList<IndexFile> files = new ArrayList<>(indexService.getTimeStoreTable().values());
            return IndexFile.IndexStatusEnum.UPLOAD.equals(files.get(0).getFileStatus());
        });
        indexService.shutdown();

        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        Assert.assertEquals(timestamp, indexService.getTimeStoreTable().firstKey().longValue());
        Assert.assertEquals(2, indexService.getTimeStoreTable().size());
        Assert.assertEquals(IndexFile.IndexStatusEnum.UPLOAD,
            indexService.getTimeStoreTable().firstEntry().getValue().getFileStatus());
    }

    @Test
    public void queryFromFileTest() throws InterruptedException, ExecutionException {
        long timestamp = System.currentTimeMillis();
        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);

        // three files, echo contains 19 items
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 20 - 1; j++) {
                AppendResult result = indexService.putKey(
                    TOPIC_NAME, TOPIC_ID, QUEUE_ID, Collections.singleton(String.valueOf(j)),
                    i * 100L + j, MESSAGE_SIZE, System.currentTimeMillis());
                Assert.assertEquals(AppendResult.SUCCESS, result);
                TimeUnit.MILLISECONDS.sleep(1);
            }
        }

        ArrayList<IndexFile> files = new ArrayList<>(indexService.getTimeStoreTable().values());
        Assert.assertEquals(3, files.size());

        for (int i = 0; i < 3; i++) {
            List<IndexItem> indexItems = indexService.queryAsync(
                TOPIC_NAME, String.valueOf(1), 1, timestamp, System.currentTimeMillis()).get();
            Assert.assertEquals(1, indexItems.size());

            indexItems = indexService.queryAsync(
                TOPIC_NAME, String.valueOf(1), 3, timestamp, System.currentTimeMillis()).get();
            Assert.assertEquals(3, indexItems.size());

            indexItems = indexService.queryAsync(
                TOPIC_NAME, String.valueOf(1), 5, timestamp, System.currentTimeMillis()).get();
            Assert.assertEquals(3, indexItems.size());
        }
    }

    @Test
    public void concurrentGetTest() throws InterruptedException {
        storeConfig.setTieredStoreIndexFileMaxIndexNum(2000);
        indexService = new IndexStoreService(messageStore, fileAllocator, filePath);
        indexService.start();

        int fileCount = 10;
        for (int j = 0; j < fileCount; j++) {
            for (int i = 0; i < storeConfig.getTieredStoreIndexFileMaxIndexNum(); i++) {
                indexService.putKey(TOPIC_NAME, TOPIC_ID, j, Collections.singleton(String.valueOf(i)),
                    i * 100L, i * 100, System.currentTimeMillis());
            }
            TimeUnit.MILLISECONDS.sleep(1);
        }

        CountDownLatch latch = new CountDownLatch(fileCount * 3);
        AtomicBoolean result = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(
            4, new ThreadFactoryImpl("ConcurrentGetTest"));

        for (int i = 0; i < fileCount; i++) {
            int finalI = i;
            executorService.submit(() -> {
                for (int j = 1; j <= 3; j++) {
                    try {
                        List<IndexItem> indexItems = indexService.queryAsync(
                            TOPIC_NAME, String.valueOf(finalI), j * 5, 0, System.currentTimeMillis()).get();
                        if (Math.min(fileCount, j * 5) != indexItems.size()) {
                            result.set(false);
                        }
                    } catch (Exception e) {
                        result.set(false);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        Assert.assertTrue(latch.await(15, TimeUnit.SECONDS));
        executorService.shutdown();
        Assert.assertTrue(result.get());
    }
}