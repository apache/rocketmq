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
package org.apache.rocketmq.tieredstore.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.index.IndexHeader;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredIndexFile {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    public static final int INDEX_FILE_BEGIN_MAGIC_CODE = 0xCCDDEEFF ^ 1880681586 + 4;
    public static final int INDEX_FILE_END_MAGIC_CODE = 0xCCDDEEFF ^ 1880681586 + 8;
    public static final int INDEX_FILE_HEADER_SIZE = 28;
    public static final int INDEX_FILE_HASH_SLOT_SIZE = 8;
    public static final int INDEX_FILE_HASH_ORIGIN_INDEX_SIZE = 32;
    public static final int INDEX_FILE_HASH_COMPACT_INDEX_SIZE = 28;

    public static final int INDEX_FILE_HEADER_MAGIC_CODE_POSITION = 0;
    public static final int INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION = 4;
    public static final int INDEX_FILE_HEADER_END_TIME_STAMP_POSITION = 12;
    public static final int INDEX_FILE_HEADER_SLOT_NUM_POSITION = 20;
    public static final int INDEX_FILE_HEADER_INDEX_NUM_POSITION = 24;

    private static final String INDEX_FILE_DIR_NAME = "tiered_index_file";
    private static final String CUR_INDEX_FILE_NAME = "0000";
    private static final String PRE_INDEX_FILE_NAME = "1111";
    private static final String COMPACT_FILE_NAME = "2222";

    private final TieredMessageStoreConfig storeConfig;
    private final TieredFlatFile flatFile;
    private final int maxHashSlotNum;
    private final int maxIndexNum;
    private final int fileMaxSize;
    private final String curFilePath;
    private final String preFilepath;
    private MappedFile preMappedFile;
    private MappedFile curMappedFile;

    private final ReentrantLock curFileLock = new ReentrantLock();
    private Future<Void> inflightCompactFuture = CompletableFuture.completedFuture(null);

    protected TieredIndexFile(TieredFileAllocator fileQueueFactory, String filePath) throws IOException {
        this.storeConfig = fileQueueFactory.getStoreConfig();
        this.flatFile = fileQueueFactory.createFlatFileForIndexFile(filePath);
        if (flatFile.getBaseOffset() == -1) {
            flatFile.setBaseOffset(0);
        }
        this.maxHashSlotNum = storeConfig.getTieredStoreIndexFileMaxHashSlotNum();
        this.maxIndexNum = storeConfig.getTieredStoreIndexFileMaxIndexNum();
        this.fileMaxSize = IndexHeader.INDEX_HEADER_SIZE
            + this.maxHashSlotNum * INDEX_FILE_HASH_SLOT_SIZE
            + this.maxIndexNum * INDEX_FILE_HASH_ORIGIN_INDEX_SIZE
            + 4;
        this.curFilePath = Paths.get(
            storeConfig.getStorePathRootDir(), INDEX_FILE_DIR_NAME, CUR_INDEX_FILE_NAME).toString();
        this.preFilepath = Paths.get(
            storeConfig.getStorePathRootDir(), INDEX_FILE_DIR_NAME, PRE_INDEX_FILE_NAME).toString();
        initFile();
        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(
            this::doScheduleTask, 10, 10, TimeUnit.SECONDS);
    }

    private void doScheduleTask() {
        try {
            curFileLock.lock();
            try {
                synchronized (TieredIndexFile.class) {
                    MappedByteBuffer mappedByteBuffer = curMappedFile.getMappedByteBuffer();
                    int indexNum = mappedByteBuffer.getInt(INDEX_FILE_HEADER_INDEX_NUM_POSITION);
                    long lastIndexTime = mappedByteBuffer.getLong(INDEX_FILE_HEADER_END_TIME_STAMP_POSITION);
                    if (indexNum > 0 &&
                        System.currentTimeMillis() - lastIndexTime >
                            storeConfig.getTieredStoreIndexFileRollingIdleInterval()) {
                        mappedByteBuffer.putInt(fileMaxSize - 4, INDEX_FILE_END_MAGIC_CODE);
                        rollingFile();
                    }
                    if (inflightCompactFuture.isDone() && preMappedFile != null && preMappedFile.isAvailable()) {
                        inflightCompactFuture = TieredStoreExecutor.compactIndexFileExecutor.submit(
                            new CompactTask(storeConfig, preMappedFile, flatFile), null);
                    }
                }
            } finally {
                curFileLock.unlock();
            }
        } catch (Throwable throwable) {
            logger.error("TieredIndexFile: submit compact index file task failed:", throwable);
        }
    }

    private static boolean isFileSealed(MappedFile mappedFile) {
        return mappedFile.getMappedByteBuffer().getInt(mappedFile.getFileSize() - 4) == INDEX_FILE_END_MAGIC_CODE;
    }

    private void initIndexFileHeader(MappedFile mappedFile) {
        MappedByteBuffer mappedByteBuffer = mappedFile.getMappedByteBuffer();
        if (mappedByteBuffer.getInt(0) != INDEX_FILE_BEGIN_MAGIC_CODE) {
            mappedByteBuffer.putInt(INDEX_FILE_HEADER_MAGIC_CODE_POSITION, INDEX_FILE_BEGIN_MAGIC_CODE);
            mappedByteBuffer.putLong(INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION, -1L);
            mappedByteBuffer.putLong(INDEX_FILE_HEADER_END_TIME_STAMP_POSITION, -1L);
            mappedByteBuffer.putInt(INDEX_FILE_HEADER_SLOT_NUM_POSITION, 0);
            mappedByteBuffer.putInt(INDEX_FILE_HEADER_INDEX_NUM_POSITION, 0);
            for (int i = 0; i < maxHashSlotNum; i++) {
                mappedByteBuffer.putInt(INDEX_FILE_HEADER_SIZE + i * INDEX_FILE_HASH_SLOT_SIZE, -1);
            }
            mappedByteBuffer.putInt(fileMaxSize - 4, -1);
        }
    }

    private void initFile() throws IOException {
        curMappedFile = new DefaultMappedFile(curFilePath, fileMaxSize);
        initIndexFileHeader(curMappedFile);
        File preFile = new File(preFilepath);
        boolean preFileExists = preFile.exists();
        if (preFileExists) {
            preMappedFile = new DefaultMappedFile(preFilepath, fileMaxSize);
        }

        if (isFileSealed(curMappedFile)) {
            if (preFileExists) {
                preFile.delete();
            }
            boolean rename = curMappedFile.renameTo(preFilepath);
            if (rename) {
                preMappedFile = curMappedFile;
                curMappedFile = new DefaultMappedFile(curFilePath, fileMaxSize);
                preFileExists = true;
            }
        }
        if (preFileExists) {
            synchronized (TieredIndexFile.class) {
                if (inflightCompactFuture.isDone()) {
                    inflightCompactFuture = TieredStoreExecutor.compactIndexFileExecutor.submit(new CompactTask(storeConfig, preMappedFile, flatFile), null);
                }
            }
        }
    }

    public AppendResult append(MessageQueue mq, int topicId, String key, long offset, int size, long timeStamp) {
        return putKey(mq, topicId, indexKeyHashMethod(buildKey(mq.getTopic(), key)), offset, size, timeStamp);
    }

    private boolean rollingFile() throws IOException {
        File preFile = new File(preFilepath);
        boolean preFileExists = preFile.exists();
        if (!preFileExists) {
            boolean rename = curMappedFile.renameTo(preFilepath);
            if (rename) {
                preMappedFile = curMappedFile;
                curMappedFile = new DefaultMappedFile(curFilePath, fileMaxSize);
                initIndexFileHeader(curMappedFile);
                tryToCompactPreFile();
                return true;
            } else {
                logger.error("TieredIndexFile#rollingFile: rename current file failed");
                return false;
            }
        }
        tryToCompactPreFile();
        return false;
    }

    private void tryToCompactPreFile() throws IOException {
        synchronized (TieredIndexFile.class) {
            if (inflightCompactFuture.isDone()) {
                inflightCompactFuture = TieredStoreExecutor.compactIndexFileExecutor.submit(new CompactTask(storeConfig, preMappedFile, flatFile), null);
            }
        }
    }

    private AppendResult putKey(MessageQueue mq, int topicId, int hashCode, long offset, int size, long timeStamp) {
        curFileLock.lock();
        try {
            if (isFileSealed(curMappedFile) && !rollingFile()) {
                return AppendResult.FILE_FULL;
            }

            MappedByteBuffer mappedByteBuffer = curMappedFile.getMappedByteBuffer();

            int slotPosition = hashCode % maxHashSlotNum;
            int slotOffset = INDEX_FILE_HEADER_SIZE + slotPosition * INDEX_FILE_HASH_SLOT_SIZE;

            int slotValue = mappedByteBuffer.getInt(slotOffset);

            long beginTimeStamp = mappedByteBuffer.getLong(INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION);
            if (beginTimeStamp == -1) {
                mappedByteBuffer.putLong(INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION, timeStamp);
                beginTimeStamp = timeStamp;
            }

            int indexCount = mappedByteBuffer.getInt(INDEX_FILE_HEADER_INDEX_NUM_POSITION);
            int indexOffset = INDEX_FILE_HEADER_SIZE + maxHashSlotNum * INDEX_FILE_HASH_SLOT_SIZE
                + indexCount * INDEX_FILE_HASH_ORIGIN_INDEX_SIZE;

            int timeDiff = (int) (timeStamp - beginTimeStamp);

            // put hash index
            mappedByteBuffer.putInt(indexOffset, hashCode);
            mappedByteBuffer.putInt(indexOffset + 4, topicId);
            mappedByteBuffer.putInt(indexOffset + 4 + 4, mq.getQueueId());
            mappedByteBuffer.putLong(indexOffset + 4 + 4 + 4, offset);
            mappedByteBuffer.putInt(indexOffset + 4 + 4 + 4 + 8, size);
            mappedByteBuffer.putInt(indexOffset + 4 + 4 + 4 + 8 + 4, timeDiff);
            mappedByteBuffer.putInt(indexOffset + 4 + 4 + 4 + 8 + 4 + 4, slotValue);

            // put hash slot
            mappedByteBuffer.putInt(slotOffset, indexCount);

            // put header
            indexCount += 1;
            mappedByteBuffer.putInt(INDEX_FILE_HEADER_INDEX_NUM_POSITION, indexCount);
            mappedByteBuffer.putLong(INDEX_FILE_HEADER_END_TIME_STAMP_POSITION, timeStamp);
            if (indexCount == maxIndexNum) {
                mappedByteBuffer.putInt(fileMaxSize - 4, INDEX_FILE_END_MAGIC_CODE);
                rollingFile();
            }
            return AppendResult.SUCCESS;
        } catch (Exception e) {
            logger.error("TieredIndexFile#putKey: put key failed:", e);
            return AppendResult.IO_ERROR;
        } finally {
            curFileLock.unlock();
        }
    }

    public CompletableFuture<List<Pair<Long, ByteBuffer>>> queryAsync(String topic, String key, long beginTime, long endTime) {
        int hashCode = indexKeyHashMethod(buildKey(topic, key));
        int slotPosition = hashCode % maxHashSlotNum;
        List<TieredFileSegment> fileSegmentList = flatFile.getFileListByTime(beginTime, endTime);
        CompletableFuture<List<Pair<Long, ByteBuffer>>> future = null;
        for (int i = fileSegmentList.size() - 1; i >= 0; i--) {
            TieredFileSegment fileSegment = fileSegmentList.get(i);
            CompletableFuture<ByteBuffer> tmpFuture = fileSegment.readAsync(INDEX_FILE_HEADER_SIZE + (long) slotPosition * INDEX_FILE_HASH_SLOT_SIZE, INDEX_FILE_HASH_SLOT_SIZE)
                .thenCompose(slotBuffer -> {
                    int indexPosition = slotBuffer.getInt();
                    if (indexPosition == -1) {
                        return CompletableFuture.completedFuture(null);
                    }

                    int indexSize = slotBuffer.getInt();
                    if (indexSize <= 0) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return fileSegment.readAsync(indexPosition, indexSize);
                });
            if (future == null) {
                future = tmpFuture.thenApply(indexBuffer -> {
                    List<Pair<Long, ByteBuffer>> result = new ArrayList<>();
                    if (indexBuffer != null) {
                        result.add(Pair.of(fileSegment.getMinTimestamp(), indexBuffer));
                    }
                    return result;
                });
            } else {
                future = future.thenCombine(tmpFuture, (indexList, indexBuffer) -> {
                    if (indexBuffer != null) {
                        indexList.add(Pair.of(fileSegment.getMinTimestamp(), indexBuffer));
                    }
                    return indexList;
                });
            }
        }
        return future == null ? CompletableFuture.completedFuture(new ArrayList<>()) : future;
    }

    public static String buildKey(String topic, String key) {
        return topic + "#" + key;
    }

    public static int indexKeyHashMethod(String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public void commit(boolean sync) {
        flatFile.commit(sync);
        if (sync) {
            try {
                inflightCompactFuture.get();
            } catch (Exception ignore) {
            }
        }
    }

    public void cleanExpiredFile(long expireTimestamp) {
        flatFile.cleanExpiredFile(expireTimestamp);
    }

    public void destroyExpiredFile() {
        flatFile.destroyExpiredFile();
    }

    public void destroy() {
        inflightCompactFuture.cancel(true);
        if (preMappedFile != null) {
            preMappedFile.destroy(-1);
        }
        if (curMappedFile != null) {
            curMappedFile.destroy(-1);
        }
        String compactFilePath = storeConfig.getStorePathRootDir() + File.separator + INDEX_FILE_DIR_NAME + File.separator + COMPACT_FILE_NAME;
        File compactFile = new File(compactFilePath);
        if (compactFile.exists()) {
            compactFile.delete();
        }
        flatFile.destroy();
    }

    static class CompactTask implements Runnable {
        private final TieredMessageStoreConfig storeConfig;

        private final int maxHashSlotNum;
        private final int maxIndexNum;
        private final int fileMaxSize;
        private MappedFile originFile;
        private TieredFlatFile fileQueue;
        private final MappedFile compactFile;

        public CompactTask(TieredMessageStoreConfig storeConfig, MappedFile originFile,
            TieredFlatFile fileQueue) throws IOException {
            this.storeConfig = storeConfig;
            this.maxHashSlotNum = storeConfig.getTieredStoreIndexFileMaxHashSlotNum();
            this.maxIndexNum = storeConfig.getTieredStoreIndexFileMaxIndexNum();
            this.originFile = originFile;
            this.fileQueue = fileQueue;
            String compactFilePath = storeConfig.getStorePathRootDir() + File.separator + INDEX_FILE_DIR_NAME + File.separator + COMPACT_FILE_NAME;
            fileMaxSize = IndexHeader.INDEX_HEADER_SIZE + (storeConfig.getTieredStoreIndexFileMaxHashSlotNum() * INDEX_FILE_HASH_SLOT_SIZE) + (storeConfig.getTieredStoreIndexFileMaxIndexNum() * INDEX_FILE_HASH_ORIGIN_INDEX_SIZE) + 4;
            // TODO check magic code, upload immediately when compact complete
            File compactFile = new File(compactFilePath);
            if (compactFile.exists()) {
                compactFile.delete();
            }
            this.compactFile = new DefaultMappedFile(compactFilePath, fileMaxSize);
        }

        @Override
        public void run() {
            try {
                compact();
            } catch (Throwable throwable) {
                logger.error("TieredIndexFile#compactTask: compact index file failed:", throwable);
            }
        }

        public void compact() {
            if (!isFileSealed(originFile)) {
                logger.error("[Bug]TieredIndexFile#CompactTask#compact: try to compact unsealed file");
                originFile.destroy(-1);
                compactFile.destroy(-1);
                return;
            }

            buildCompactFile();
            fileQueue.append(compactFile.getMappedByteBuffer());
            fileQueue.commit(true);
            compactFile.destroy(-1);
            originFile.destroy(-1);
        }

        private void buildCompactFile() {
            MappedByteBuffer originMappedByteBuffer = originFile.getMappedByteBuffer();
            MappedByteBuffer compactMappedByteBuffer = compactFile.getMappedByteBuffer();
            compactMappedByteBuffer.putInt(INDEX_FILE_HEADER_MAGIC_CODE_POSITION, INDEX_FILE_BEGIN_MAGIC_CODE);
            compactMappedByteBuffer.putLong(INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION, originMappedByteBuffer.getLong(INDEX_FILE_HEADER_BEGIN_TIME_STAMP_POSITION));
            compactMappedByteBuffer.putLong(INDEX_FILE_HEADER_END_TIME_STAMP_POSITION, originMappedByteBuffer.getLong(INDEX_FILE_HEADER_END_TIME_STAMP_POSITION));
            compactMappedByteBuffer.putInt(INDEX_FILE_HEADER_SLOT_NUM_POSITION, maxHashSlotNum);
            compactMappedByteBuffer.putInt(INDEX_FILE_HEADER_INDEX_NUM_POSITION, originMappedByteBuffer.getInt(INDEX_FILE_HEADER_INDEX_NUM_POSITION));

            int rePutSlotValue = INDEX_FILE_HEADER_SIZE + (maxHashSlotNum * INDEX_FILE_HASH_SLOT_SIZE);
            for (int i = 0; i < maxHashSlotNum; i++) {
                int slotOffset = INDEX_FILE_HEADER_SIZE + i * INDEX_FILE_HASH_SLOT_SIZE;
                int slotValue = originMappedByteBuffer.getInt(slotOffset);
                if (slotValue != -1) {
                    int indexTotalSize = 0;
                    int indexPosition = slotValue;
                    while (indexPosition >= 0 && indexPosition < maxIndexNum) {
                        int indexOffset = INDEX_FILE_HEADER_SIZE + maxHashSlotNum * INDEX_FILE_HASH_SLOT_SIZE
                            + indexPosition * INDEX_FILE_HASH_ORIGIN_INDEX_SIZE;
                        int rePutIndexOffset = rePutSlotValue + indexTotalSize;

                        compactMappedByteBuffer.putInt(rePutIndexOffset, originMappedByteBuffer.getInt(indexOffset));
                        compactMappedByteBuffer.putInt(rePutIndexOffset + 4, originMappedByteBuffer.getInt(indexOffset + 4));
                        compactMappedByteBuffer.putInt(rePutIndexOffset + 4 + 4, originMappedByteBuffer.getInt(indexOffset + 4 + 4));
                        compactMappedByteBuffer.putLong(rePutIndexOffset + 4 + 4 + 4, originMappedByteBuffer.getLong(indexOffset + 4 + 4 + 4));
                        compactMappedByteBuffer.putInt(rePutIndexOffset + 4 + 4 + 4 + 8, originMappedByteBuffer.getInt(indexOffset + 4 + 4 + 4 + 8));
                        compactMappedByteBuffer.putInt(rePutIndexOffset + 4 + 4 + 4 + 8 + 4, originMappedByteBuffer.getInt(indexOffset + 4 + 4 + 4 + 8 + 4));

                        indexTotalSize += INDEX_FILE_HASH_COMPACT_INDEX_SIZE;
                        indexPosition = originMappedByteBuffer.getInt(indexOffset + 4 + 4 + 4 + 8 + 4 + 4);
                    }
                    compactMappedByteBuffer.putInt(slotOffset, rePutSlotValue);
                    compactMappedByteBuffer.putInt(slotOffset + 4, indexTotalSize);
                    rePutSlotValue += indexTotalSize;
                }
            }
            compactMappedByteBuffer.putInt(INDEX_FILE_HEADER_MAGIC_CODE_POSITION, INDEX_FILE_END_MAGIC_CODE);
            compactMappedByteBuffer.putInt(rePutSlotValue, INDEX_FILE_BEGIN_MAGIC_CODE);
            compactMappedByteBuffer.limit(rePutSlotValue + 4);
        }
    }
}
