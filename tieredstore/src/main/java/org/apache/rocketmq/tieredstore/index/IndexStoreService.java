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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.file.TieredFileAllocator;
import org.apache.rocketmq.tieredstore.file.TieredFlatFile;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class IndexStoreService extends ServiceThread implements IndexService {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    public static final String FILE_DIRECTORY_NAME = "tiered_index_file";
    public static final String FILE_COMPACTED_DIRECTORY_NAME = "compacting";

    /**
     * File status in table example:
     * upload, upload, upload, sealed, sealed, unsealed
     */
    private final TieredMessageStoreConfig storeConfig;
    private final ConcurrentSkipListMap<Long /* timestamp */, IndexFile> timeStoreTable;
    private final ReadWriteLock readWriteLock;
    private final AtomicLong compactTimestamp;
    private final String filePath;
    private final TieredFileAllocator fileAllocator;

    private IndexFile currentWriteFile;
    private TieredFlatFile flatFile;

    public IndexStoreService(TieredFileAllocator fileAllocator, String filePath) {
        this.storeConfig = fileAllocator.getStoreConfig();
        this.filePath = filePath;
        this.fileAllocator = fileAllocator;
        this.timeStoreTable = new ConcurrentSkipListMap<>();
        this.compactTimestamp = new AtomicLong(0L);
        this.readWriteLock = new ReentrantReadWriteLock();
        this.recover();
    }

    private void doConvertOldFormatFile(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                return;
            }
            MappedFile mappedFile = new DefaultMappedFile(file.getPath(), (int) file.length());
            long timestamp = mappedFile.getMappedByteBuffer().getLong(IndexStoreFile.INDEX_BEGIN_TIME_STAMP);
            if (timestamp <= 0) {
                mappedFile.destroy(TimeUnit.SECONDS.toMillis(10));
            } else {
                mappedFile.renameTo(String.valueOf(new File(file.getParent(), String.valueOf(timestamp))));
                mappedFile.shutdown(TimeUnit.SECONDS.toMillis(10));
            }
        } catch (Exception e) {
            log.error("IndexStoreService do convert old format error, file: {}", filePath, e);
        }
    }

    private void recover() {
        Stopwatch stopwatch = Stopwatch.createStarted();

        // delete compact file directory
        UtilAll.deleteFile(new File(Paths.get(storeConfig.getStorePathRootDir(),
            FILE_DIRECTORY_NAME, FILE_COMPACTED_DIRECTORY_NAME).toString()));

        // recover local
        File dir = new File(Paths.get(storeConfig.getStorePathRootDir(), FILE_DIRECTORY_NAME).toString());
        this.doConvertOldFormatFile(Paths.get(dir.getPath(), "0000").toString());
        this.doConvertOldFormatFile(Paths.get(dir.getPath(), "1111").toString());
        File[] files = dir.listFiles();

        if (files != null) {
            List<File> fileList = Arrays.asList(files);
            fileList.sort(Comparator.comparing(File::getName));

            for (File file : fileList) {
                if (file.isDirectory() || !StringUtils.isNumeric(file.getName())) {
                    continue;
                }

                try {
                    IndexFile indexFile = new IndexStoreFile(storeConfig, Long.parseLong(file.getName()));
                    timeStoreTable.put(indexFile.getTimestamp(), indexFile);
                    log.info("IndexStoreService recover load local file, timestamp: {}", indexFile.getTimestamp());
                } catch (Exception e) {
                    log.error("IndexStoreService recover, load local file error", e);
                }
            }
        }

        if (this.timeStoreTable.isEmpty()) {
            this.createNewIndexFile(System.currentTimeMillis());
        }

        this.currentWriteFile = this.timeStoreTable.lastEntry().getValue();
        this.setCompactTimestamp(this.timeStoreTable.firstKey() - 1);

        // recover remote
        this.flatFile = fileAllocator.createFlatFileForIndexFile(filePath);
        if (this.flatFile.getBaseOffset() == -1) {
            this.flatFile.setBaseOffset(0);
        }

        for (TieredFileSegment fileSegment : flatFile.getFileSegmentList()) {
            IndexFile indexFile = new IndexStoreFile(storeConfig, fileSegment);
            IndexFile localFile = timeStoreTable.get(indexFile.getTimestamp());
            if (localFile != null) {
                localFile.destroy();
            }
            timeStoreTable.put(indexFile.getTimestamp(), indexFile);
            log.info("IndexStoreService recover load remote file, timestamp: {}", indexFile.getTimestamp());
        }

        log.info("IndexStoreService recover finished, entrySize: {}, cost: {}ms, directory: {}",
            timeStoreTable.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS), dir.getAbsolutePath());
    }

    public void createNewIndexFile(long timestamp) {
        try {
            this.readWriteLock.writeLock().lock();
            IndexFile indexFile = this.currentWriteFile;
            if (this.timeStoreTable.containsKey(timestamp) ||
                indexFile != null && IndexFile.IndexStatusEnum.UNSEALED.equals(indexFile.getFileStatus())) {
                return;
            }
            IndexStoreFile newStoreFile = new IndexStoreFile(storeConfig, timestamp);
            this.timeStoreTable.put(timestamp, newStoreFile);
            this.currentWriteFile = newStoreFile;
            log.info("IndexStoreService construct next file, timestamp: {}", timestamp);
        } catch (Exception e) {
            log.error("IndexStoreService construct next file, timestamp: {}", timestamp, e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    public ConcurrentSkipListMap<Long, IndexFile> getTimeStoreTable() {
        return timeStoreTable;
    }

    @Override
    public AppendResult putKey(
        String topic, int topicId, int queueId, Set<String> keySet, long offset, int size, long timestamp) {

        if (StringUtils.isBlank(topic)) {
            return AppendResult.UNKNOWN_ERROR;
        }

        if (keySet == null || keySet.isEmpty()) {
            return AppendResult.SUCCESS;
        }

        for (int i = 0; i < 3; i++) {
            AppendResult result = this.currentWriteFile.putKey(
                topic, topicId, queueId, keySet, offset, size, timestamp);

            if (AppendResult.SUCCESS.equals(result)) {
                return AppendResult.SUCCESS;
            } else if (AppendResult.FILE_FULL.equals(result)) {
                this.createNewIndexFile(timestamp);
            }
        }

        log.error("IndexStoreService put key three times return error, topic: {}, topicId: {}, " +
            "queueId: {}, keySize: {}, timestamp: {}", topic, topicId, queueId, keySet.size(), timestamp);
        return AppendResult.UNKNOWN_ERROR;
    }

    @Override
    public CompletableFuture<List<IndexItem>> queryAsync(
        String topic, String key, int maxCount, long beginTime, long endTime) {

        CompletableFuture<List<IndexItem>> future = new CompletableFuture<>();
        try {
            readWriteLock.readLock().lock();
            ConcurrentNavigableMap<Long, IndexFile> pendingMap =
                this.timeStoreTable.subMap(beginTime, true, endTime, true);
            List<CompletableFuture<Void>> futureList = new ArrayList<>(pendingMap.size());
            ConcurrentHashMap<String /* queueId-offset */, IndexItem> result = new ConcurrentHashMap<>();

            for (Map.Entry<Long, IndexFile> entry : pendingMap.descendingMap().entrySet()) {
                CompletableFuture<Void> completableFuture = entry.getValue()
                    .queryAsync(topic, key, maxCount, beginTime, endTime)
                    .thenAccept(itemList -> itemList.forEach(indexItem -> {
                        if (result.size() < maxCount) {
                            result.put(String.format(
                                "%d-%d", indexItem.getQueueId(), indexItem.getOffset()), indexItem);
                        }
                    }));
                futureList.add(completableFuture);
            }

            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
                .whenComplete((v, t) -> {
                    // Try to return the query results as much as possible here
                    // rather than directly throwing exceptions
                    if (result.isEmpty() && t != null) {
                        future.completeExceptionally(t);
                    } else {
                        List<IndexItem> resultList = new ArrayList<>(result.values());
                        future.complete(resultList.subList(0, Math.min(resultList.size(), maxCount)));
                    }
                });
        } catch (Exception e) {
            future.completeExceptionally(e);
        } finally {
            readWriteLock.readLock().unlock();
        }
        return future;
    }

    public void doCompactThenUploadFile(IndexFile indexFile) {
        if (IndexFile.IndexStatusEnum.UPLOAD.equals(indexFile.getFileStatus())) {
            log.error("IndexStoreService file status not correct, so skip, timestamp: {}, status: {}",
                indexFile.getTimestamp(), indexFile.getFileStatus());
            indexFile.destroy();
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        ByteBuffer byteBuffer = indexFile.doCompaction();
        if (byteBuffer == null) {
            log.error("IndexStoreService found compaction buffer is null, timestamp: {}", indexFile.getTimestamp());
            return;
        }
        flatFile.append(byteBuffer);
        flatFile.commit(true);

        TieredFileSegment fileSegment = flatFile.getFileByIndex(flatFile.getFileSegmentCount() - 1);
        if (fileSegment == null || fileSegment.getMinTimestamp() != indexFile.getTimestamp()) {
            log.warn("IndexStoreService submit compacted file to server failed, timestamp: {}", indexFile.getTimestamp());
            return;
        }

        try {
            readWriteLock.writeLock().lock();
            IndexFile storeFile = new IndexStoreFile(storeConfig, fileSegment);
            timeStoreTable.put(indexFile.getTimestamp(), storeFile);
            indexFile.destroy();
        } catch (Exception e) {
            log.error("IndexStoreService switch file failed, timestamp: {}, cost: {}ms",
                indexFile.getTimestamp(), stopwatch.elapsed(TimeUnit.MILLISECONDS), e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void destroyExpiredFile(long expireTimestamp) {
        flatFile.cleanExpiredFile(expireTimestamp);
        flatFile.destroyExpiredFile();
    }

    public void destroy() {
        try {
            readWriteLock.writeLock().lock();

            // delete local store file
            for (Map.Entry<Long, IndexFile> entry : timeStoreTable.entrySet()) {
                IndexFile indexFile = entry.getValue();
                if (IndexFile.IndexStatusEnum.UPLOAD.equals(indexFile.getFileStatus())) {
                    continue;
                }
                indexFile.destroy();
            }

            // delete remote
            if (flatFile != null) {
                flatFile.destroy();
            }
        } catch (Exception e) {
            log.error("IndexStoreService destroy all file error", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public String getServiceName() {
        return IndexStoreService.class.getSimpleName();
    }

    public void setCompactTimestamp(long timestamp) {
        this.compactTimestamp.set(timestamp);
        log.info("IndexStoreService compact timestamp has been set to: {}", timestamp);
    }

    protected IndexFile getNextSealedFile() {
        try {
            Map.Entry<Long, IndexFile> entry =
                this.timeStoreTable.higherEntry(this.compactTimestamp.get());
            if (entry != null && entry.getKey() < this.timeStoreTable.lastKey()) {
                return entry.getValue();
            }
        } catch (Throwable e) {
            log.error("Error occurred in " + getServiceName(), e);
        }
        return null;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStopped()) {
            long expireTimestamp = System.currentTimeMillis()
                - TimeUnit.HOURS.toMillis(storeConfig.getTieredStoreFileReservedTime());
            this.destroyExpiredFile(expireTimestamp);

            IndexFile indexFile = this.getNextSealedFile();
            if (indexFile == null) {
                this.waitForRunning(TimeUnit.SECONDS.toMillis(10));
                continue;
            }
            this.doCompactThenUploadFile(indexFile);
            this.setCompactTimestamp(indexFile.getTimestamp());
        }
        log.info(this.getServiceName() + " service shutdown");
    }

    @Override
    public void shutdown() {
        super.shutdown();
        for (Map.Entry<Long /* timestamp */, IndexFile> entry : timeStoreTable.entrySet()) {
            entry.getValue().shutdown();
        }
        this.timeStoreTable.clear();
        log.info("IndexStoreService shutdown gracefully");
    }
}
