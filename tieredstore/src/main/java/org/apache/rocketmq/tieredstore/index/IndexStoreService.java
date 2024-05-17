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
import java.util.LinkedList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.file.FlatAppendFile;
import org.apache.rocketmq.tieredstore.file.FlatFileFactory;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexStoreService extends ServiceThread implements IndexService {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    public static final String FILE_DIRECTORY_NAME = "tiered_index_file";
    public static final String FILE_COMPACTED_DIRECTORY_NAME = "compacting";

    /**
     * File status in table example:
     * upload, upload, upload, sealed, sealed, unsealed
     */
    private final MessageStoreConfig storeConfig;
    private final ConcurrentSkipListMap<Long /* timestamp */, IndexFile> timeStoreTable;
    private final ReadWriteLock readWriteLock;
    private final AtomicLong compactTimestamp;
    private final String filePath;
    private final FlatFileFactory fileAllocator;
    private final TieredMessageStore messageStore;
    private final ReentrantLock masterListLock;
    private final ReentrantLock slaveListLock;
    /**
     * For master: add DispatchRequest to synchronize with slave
     * For slave: accept DispatchRequest and putKey to local
     * */
    private final List<DispatchRequestExt> masterDispatchRequestList;
    private List<DispatchRequestExt> slaveDispatchRequestList;

    private IndexFile currentWriteFile;
    private FlatAppendFile flatAppendFile;

    public IndexStoreService(TieredMessageStore messageStore, FlatFileFactory flatFileFactory, String filePath) {
        this.messageStore = messageStore;
        this.storeConfig = flatFileFactory.getStoreConfig();
        this.filePath = filePath;
        this.fileAllocator = flatFileFactory;
        this.timeStoreTable = new ConcurrentSkipListMap<>();
        this.compactTimestamp = new AtomicLong(0L);
        this.readWriteLock = new ReentrantReadWriteLock();
        this.masterDispatchRequestList = new LinkedList<>();
        this.slaveDispatchRequestList = new LinkedList<>();
        this.masterListLock = new ReentrantLock();
        this.slaveListLock = new ReentrantLock();
        this.recover();
    }

    @Override
    public void recoverWhenBecomeMaster() {
        while (!slaveDispatchRequestList.isEmpty()) {
            emptySlaveDispatchRequestList();
        }
        recover();
    }

    @Override
    public List<Long> getIndexFileTimestamp() {
        return new ArrayList<>(timeStoreTable.keySet());
    }

    public List<DispatchRequestExt> getDispatchRequestToSync() {
        List<DispatchRequestExt> result;
        masterListLock.lock();
        try {
            int num = Math.min(this.storeConfig.getDispatchRequestGroupSyncCount(), masterDispatchRequestList.size());
            if (num > 0) {
                result = new LinkedList<>(masterDispatchRequestList.subList(0, num));
                masterDispatchRequestList.subList(0, num).clear();
            } else {
                result = Collections.emptyList();
            }
        } finally {
            masterListLock.unlock();
        }
        return result;
    }

    @Override
    public boolean needToSync() {
        return masterDispatchRequestList.size() > storeConfig.getDispatchRequestGroupSyncCount();
    }


    @Override
    public void syncDispatchRequest(List<DispatchRequestExt> requestExtList) {
        slaveListLock.lock();
        try {
            slaveDispatchRequestList.addAll(requestExtList);
        } finally {
            slaveListLock.unlock();
        }
    }
    @Override
    public void emptySlaveDispatchRequestList() {
        List<DispatchRequestExt> requestExtListCopy = new LinkedList<>();
        slaveListLock.lock();
        try {
            requestExtListCopy.addAll(slaveDispatchRequestList);
            slaveDispatchRequestList = new LinkedList<>();
        } finally {
            slaveListLock.unlock();
        }
        log.debug("Slave dispatch {} requests to build IndexFile", requestExtListCopy.size());
        requestExtListCopy.forEach(requestExt -> {
            DispatchRequest request = requestExt.getDispatchRequest();
            this.putKey(request.getTopic(), (int) requestExt.getTopicId(), request.getQueueId(), requestExt.getKeySet(),
                    request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
        });
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
        this.flatAppendFile = fileAllocator.createFlatFileForIndexFile(filePath);

        for (FileSegment fileSegment : flatAppendFile.getFileSegmentList()) {
            IndexFile indexFile = new IndexStoreFile(storeConfig, fileSegment);
            long timestamp = indexFile.getTimestamp();
            // for recoverWhenBecomeMaster
            if (!timeStoreTable.containsKey(timestamp)) {
                // higherTimestamp and lowerTimestamp will not be null at same time
                Long higherTimestamp = timeStoreTable.higherKey(timestamp);
                Long lowerTimestamp = timeStoreTable.lowerKey(timestamp);
                Long removeTimestamp;
                if (higherTimestamp == null) {
                    removeTimestamp = lowerTimestamp;
                } else if (lowerTimestamp == null) {
                    removeTimestamp = higherTimestamp;
                } else {
                    removeTimestamp =
                        higherTimestamp - timestamp < timestamp - lowerTimestamp ? higherTimestamp : lowerTimestamp;
                }
                if (Math.abs(removeTimestamp - timestamp) < 3 * storeConfig.getSlaveBuildIndexInterval() &&
                    removeTimestamp != this.currentWriteFile.getTimestamp()) {
                    IndexFile removeFile = timeStoreTable.remove(removeTimestamp);
                    log.info("IndexStoreService remove dirty local data, remove timestamp: {}, status: {}",
                        removeTimestamp, removeFile.getFileStatus());
                    removeFile.destroy();
                }
            }
            timeStoreTable.put(indexFile.getTimestamp(), indexFile);
            log.info("IndexStoreService recover load remote file, timestamp: {}, end timestamp: {}",
                indexFile.getTimestamp(), indexFile.getEndTimestamp());
        }

        log.info("IndexStoreService recover finished, total: {}, cost: {}ms, directory: {}",
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
                if (MessageStoreUtil.isMaster(messageStore.getDefaultStore().getMessageStoreConfig())) {
                    DispatchRequest dispatchRequest = new DispatchRequest(
                            topic, queueId, offset, size,
                            0L, timestamp, 0L, "", "",
                            0, 0, new HashMap<>());
                    try {
                        masterListLock.lock();
                        this.masterDispatchRequestList.add(new DispatchRequestExt(dispatchRequest, topicId, keySet));
                    } finally {
                        masterListLock.unlock();
                    }
                }
                return AppendResult.SUCCESS;
            } else if (AppendResult.FILE_FULL.equals(result)) {
                // use current time to ensure the order of file
                this.createNewIndexFile(System.currentTimeMillis());
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

    public boolean doCompactThenUploadFile(IndexFile indexFile) {
        if (IndexFile.IndexStatusEnum.UPLOAD.equals(indexFile.getFileStatus())) {
            log.error("IndexStoreService file status not correct, so skip, timestamp: {}, status: {}",
                indexFile.getTimestamp(), indexFile.getFileStatus());
            indexFile.destroy();
            return true;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        if (MessageStoreUtil.isMaster(this.messageStore.getDefaultStore().getMessageStoreConfig())) {
            if (flatAppendFile.getCommitOffset() == flatAppendFile.getAppendOffset()) {
                ByteBuffer byteBuffer = indexFile.doCompaction();
                if (byteBuffer == null) {
                    log.error("IndexStoreService found compaction buffer is null, timestamp: {}", indexFile.getTimestamp());
                    return false;
                }
                flatAppendFile.rollingNewFile(Math.max(0L, flatAppendFile.getAppendOffset()));
                flatAppendFile.append(byteBuffer, indexFile.getTimestamp());
                flatAppendFile.getFileToWrite().setMinTimestamp(indexFile.getTimestamp());
                flatAppendFile.getFileToWrite().setMaxTimestamp(indexFile.getEndTimestamp());
            }
            boolean result = flatAppendFile.commitAsync().join();

            List<FileSegment> fileSegmentList = flatAppendFile.getFileSegmentList();
            FileSegment fileSegment = fileSegmentList.get(fileSegmentList.size() - 1);
            if (!result || fileSegment == null || fileSegment.getMinTimestamp() != indexFile.getTimestamp()) {
                log.warn("IndexStoreService upload compacted file error, timestamp: {}", indexFile.getTimestamp());
                return false;
            } else {
                log.info("IndexStoreService upload compacted file success, timestamp: {}", indexFile.getTimestamp());
            }

            readWriteLock.writeLock().lock();
            try {
                IndexFile storeFile = new IndexStoreFile(storeConfig, fileSegment);
                timeStoreTable.put(storeFile.getTimestamp(), storeFile);
                indexFile.destroy();
            } catch (Exception e) {
                log.error("IndexStoreService rolling file error, timestamp: {}, cost: {}ms",
                        indexFile.getTimestamp(), stopwatch.elapsed(TimeUnit.MILLISECONDS), e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            readWriteLock.writeLock().lock();
            try {
                timeStoreTable.remove(indexFile.getTimestamp());
                indexFile.destroy();
            } catch (Exception e) {
                log.error("IndexStoreService remove file error, timestamp: {}, cost: {}ms",
                        indexFile.getTimestamp(), stopwatch.elapsed(TimeUnit.MILLISECONDS), e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
            log.info("IndexStoreService Slave remove SEALED file, timestamp = {}", indexFile.getTimestamp());
        }
        return true;
    }

    public void destroyExpiredFile(long expireTimestamp) {
        // delete file in time store table
        readWriteLock.writeLock().lock();
        try {
            timeStoreTable.entrySet().removeIf(entry ->
                entry.getKey() < expireTimestamp &&
                    IndexFile.IndexStatusEnum.UPLOAD.equals(entry.getValue().getFileStatus()));
            flatAppendFile.destroyExpiredFile(expireTimestamp);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void destroy() {
        readWriteLock.writeLock().lock();
        try {
            // delete local store file
            for (Map.Entry<Long, IndexFile> entry : timeStoreTable.entrySet()) {
                IndexFile indexFile = entry.getValue();
                if (IndexFile.IndexStatusEnum.UPLOAD.equals(indexFile.getFileStatus())) {
                    continue;
                }
                indexFile.destroy();
            }
            // delete remote
            if (flatAppendFile != null) {
                flatAppendFile.destroy();
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
        log.debug("IndexStoreService set compact timestamp to: {}", timestamp);
    }

    protected IndexFile getNextSealedFile() {
        Map.Entry<Long, IndexFile> entry =
            this.timeStoreTable.higherEntry(this.compactTimestamp.get());
        if (entry != null && entry.getKey() < this.timeStoreTable.lastKey()) {
            return entry.getValue();
        }
        return null;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        readWriteLock.writeLock().lock();
        try {
            for (Map.Entry<Long /* timestamp */, IndexFile> entry : timeStoreTable.entrySet()) {
                entry.getValue().shutdown();
            }
            this.timeStoreTable.clear();
        } catch (Exception e) {
            log.error("IndexStoreService shutdown error", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            long expireTimestamp = System.currentTimeMillis()
                - TimeUnit.HOURS.toMillis(storeConfig.getTieredStoreFileReservedTime());
            this.destroyExpiredFile(expireTimestamp);

            IndexFile indexFile = this.getNextSealedFile();
            if (indexFile != null) {
                log.info("Get next upload file timestamp = {}, status = {}", indexFile.getTimestamp(), indexFile.getFileStatus());
                if (this.doCompactThenUploadFile(indexFile)) {
                    this.setCompactTimestamp(indexFile.getTimestamp());
                    continue;
                }
            }
            this.waitForRunning(TimeUnit.SECONDS.toMillis(10));
        }
        log.info(this.getServiceName() + " service shutdown");
    }
}
