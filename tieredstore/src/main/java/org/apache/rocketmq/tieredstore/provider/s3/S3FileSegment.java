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

package org.apache.rocketmq.tieredstore.provider.s3;

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class S3FileSegment extends TieredFileSegment {

    private static final Logger LOGGER = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    /**
     * The path of the file segment in S3. Format:
     * <pre>
     *     ${hash of clusterName}/${clusterName}/${brokerName}/${topicName}/${queueId}/${fileType}/seg-${baseOffset}
     * </pre>
     */
    private final String storePath;

    /**
     * The path of the chunk file in S3. Format:
     * <pre>
     *     {@link #storePath}/chunk
     * </pre>
     */
    private final String chunkPath;

    /**
     * The path of the segment file in S3. Format:
     * <pre>
     *     {@link #storePath}/segment
     * </pre>
     */
    private final String segmentPath;

    private final TieredStorageS3Client client;

    private final S3FileSegmentMetadata metadata;

    /**
     * Executor for merging chunks into segment or deleting chunks.
     * <p>
     * TODO: Better to use a thread pool.
     */
    private static final ExecutorService MERGE_CHUNKS_INTO_SEGMENT_EXECUTOR = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("S3FileSegment_MergeChunksIntoSegmentExecutor"));

    // TODO: Uses the specified asynchronous thread pool

    public S3FileSegment(FileSegmentType fileType, MessageQueue messageQueue, long baseOffset, TieredMessageStoreConfig storeConfig) {
        super(fileType, messageQueue, baseOffset, storeConfig);
        String clusterName = storeConfig.getBrokerClusterName();
        String hash = String.valueOf(clusterName.hashCode());
        this.storePath = hash + File.separator + clusterName + File.separator + messageQueue.getBrokerName() +
                File.separator + messageQueue.getTopic() + File.separator + messageQueue.getQueueId() + File.separator + fileType + File.separator + "seg-" + baseOffset;
        this.chunkPath = this.storePath + File.separator + "chunk";
        this.segmentPath = this.storePath + File.separator + "segment";
        this.client = TieredStorageS3Client.getInstance(storeConfig);
        this.metadata = new S3FileSegmentMetadata();
        this.initialize();
    }

    private void initialize() {
        // check if the file segment exists
        CompletableFuture<List<ChunkMetadata>> listSegments = this.client.listChunks(this.segmentPath);
        CompletableFuture<List<ChunkMetadata>> listChunks = this.client.listChunks(this.chunkPath);
        List<ChunkMetadata> segments = listSegments.join();
        if (segments.size() > 1) {
            throw new RuntimeException("The segment " + segmentPath + " should be only one, but now have " + segments.size() + " segments, please check it.");
        }
        List<ChunkMetadata> chunks = listChunks.join();
        if (segments.size() == 1) {
            // now segment exist
            // add segment into metadata
            ChunkMetadata segment = segments.get(0);
            this.metadata.setSegment(segment);
            // delete chunks
            this.client.deleteObjets(chunks.stream().map(chunk -> chunk.getChunkName()).collect(Collectors.toList())).join();
        } else {
            // now segment not exist
            // add all chunks into metadata
            checkAndLoadChunks(chunks);
        }
    }

    private void checkAndLoadChunks(List<ChunkMetadata> chunks) {
        if (chunks.size() == 0) {
            return;
        }
        for (ChunkMetadata chunk : chunks) {
            if (!this.metadata.addChunk(chunk)) {
                // the chunk is not valid
                LOGGER.error("The chunk: {} is not valid, now chunks last end position: {}, please check it.", chunk, this.metadata.getEndPosition());
                throw new RuntimeException("The chunk: " + chunk + " is not valid, now chunks last end position: " + this.metadata.getEndPosition() + ", please check it.");
            }
        }
    }

    @Override
    public String getPath() {
        return this.storePath;
    }

    public String getSegmentPath() {
        return segmentPath;
    }

    public String getChunkPath() {
        return chunkPath;
    }

    @Override
    public long getSize() {
        return this.metadata.getSize();
    }

    @Override
    public boolean exists() {
        return this.client.exist(this.storePath).join();
    }

    @Override
    public void createFile() {

    }

    /**
     * Merges all normal chunks into a segment file.
     */
    @Override
    public void sealFile() {
        // check if the segment file exists
        if (this.metadata.isSealed() && this.metadata.getChunkCount() == 0) {
            return;
        }
        // merge all chunks into a segment file and delete all chunks
        MERGE_CHUNKS_INTO_SEGMENT_EXECUTOR.submit(this::trySealFile);
    }

    private void trySealFile() {
        while (true) {
            if (this.metadata.isSealed() && this.metadata.getChunkCount() == 0) return;

            boolean success = true;

            if (!this.metadata.isSealed()) {
                // merge all chunks
                String segmentName = this.segmentPath + File.separator + "segment-" + 0;
                boolean merged = this.client.mergeAllChunksIntoSegment(this.metadata.getChunks(), segmentName).join();
                if (merged) {
                    // set segment
                    this.metadata.setSegment(new ChunkMetadata(segmentName, 0, (int) this.metadata.getSize()));
                } else {
                    LOGGER.error("Merge chunks into segment failed, chunk path is {}, segment path is {}.", this.chunkPath, this.segmentPath);
                    success = false;
                }
            }
            if (success) {
                // old chunks still exist, keep deleting them
                List<String> chunkKeys = this.metadata.getChunks().stream().map(metadata -> metadata.getChunkName()).collect(Collectors.toList());
                List<String> undeleteList = this.client.deleteObjets(chunkKeys).join();
                if (undeleteList.isEmpty()) {
                    this.metadata.removeAllChunks();
                } else {
                    success = false;
                    LOGGER.error("Delete chunks failed, chunk path is {}, undelete list is {}.", this.chunkPath, undeleteList);
                }
            }
            if (success) return;
            // unsuccessful, retry
            try {
                Thread.sleep(1000);
            } catch (Exception ignore) {

            }

        }
    }

    public boolean isSealed() {
        return this.metadata.isSealed();
    }

    @Override
    public void destroyFile() {
        this.client.deleteObjects(this.storePath).join();
        this.metadata.clear();
    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        CompletableFuture<ByteBuffer> completableFuture = new CompletableFuture<>();
        List<ChunkMetadata> chunks = null;
        try {
            chunks = this.metadata.seek(position, length);
        } catch (IndexOutOfBoundsException e) {
            LOGGER.error("Read position {} and length {} out of range, the file segment size is {}.", position, length, this.metadata.getSize());
            completableFuture.completeExceptionally(new TieredStoreException(TieredStoreErrorCode.DOWNLOAD_LENGTH_NOT_CORRECT, "read data from segment error because of position or length not correct"));
            return completableFuture;
        }
        long endPosition = position + length - 1;
        ConcurrentByteBuffer concurrentByteBuffer = new ConcurrentByteBuffer(length);
        List<CompletableFuture<byte[]>> subFutures = new ArrayList<>(chunks.size());
        long startPosition = chunks.get(0).getStartPosition();
        chunks.forEach(chunk -> {
            long chunkStartPosition = position >= chunk.getStartPosition() ? position - chunk.getStartPosition() : 0;
            long chunkEndPosition = endPosition <= chunk.getEndPosition() ? endPosition - chunk.getStartPosition() : chunk.getChunkSize() - 1;
            CompletableFuture<byte[]> future = this.client.readChunk(chunk.getChunkName(), chunkStartPosition, chunkEndPosition);
            CompletableFuture<byte[]> subFuture = future.whenComplete((bytes, throwable) -> {
                if (throwable != null) {
                    LOGGER.error("Failed to read data from s3, chunk: {}, start position: {}, end position: {}", chunk, chunkStartPosition, chunkEndPosition, throwable);
                    TieredStoreException exception = new TieredStoreException(TieredStoreErrorCode.IO_ERROR, "read data from s3 error");
                    completableFuture.completeExceptionally(exception);
                } else {
                    try {
                        concurrentByteBuffer.put(bytes, 0, bytes.length, (int) (chunk.getStartPosition() - startPosition));
                    } catch (Exception e) {
                        LOGGER.error("Failed to put data from s3 into buffer, chunk: {}, start position: {}, end position: {}", chunk, chunkStartPosition, chunkEndPosition, e);
                        TieredStoreException exception = new TieredStoreException(TieredStoreErrorCode.UNKNOWN, "put data from s3 into buffer error");
                        completableFuture.completeExceptionally(exception);
                    }
                }
            });
            subFutures.add(subFuture);
        });
        CompletableFuture.allOf(subFutures.toArray(new CompletableFuture[chunks.size()])).whenComplete((v, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Failed to read data from s3, position: {}, length: {}", position, length, throwable);
                completableFuture.completeExceptionally(new TieredStoreException(TieredStoreErrorCode.IO_ERROR, "wait all sub download tasks complete error"));
            } else {
                ByteBuffer byteBuffer = concurrentByteBuffer.close();
                byteBuffer.rewind();
                completableFuture.complete(byteBuffer);
            }
        });
        return completableFuture;
    }

    @Override
    public CompletableFuture<Boolean> commit0(InputStream inputStream, long position, int length, boolean append) {
        // TODO: Deal with the case that the param: append is false
        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        // check if now the segment is sealed
        if (this.metadata.isSealed()) {
            LOGGER.error("The segment is sealed, the position: {}, the length: {}.", position, length);
            TieredStoreException exception = new TieredStoreException(TieredStoreErrorCode.SEGMENT_SEALED, "the segment is sealed");
            exception.setPosition(this.metadata.getEndPosition());
            completableFuture.completeExceptionally(exception);
            return completableFuture;
        }
        // check if the position is valid
        if (length < 0 || position != this.metadata.getEndPosition() + 1) {
            LOGGER.error("The position is invalid, the position: {}, the length: {}.", position, length);
            TieredStoreException exception = new TieredStoreException(TieredStoreErrorCode.ILLEGAL_OFFSET, "the position is invalid");
            exception.setPosition(this.metadata.getEndPosition());
            completableFuture.completeExceptionally(exception);
            return completableFuture;
        }
        // upload chunk
        String chunkPath = this.chunkPath + File.separator + "chunk-" + position;
        this.client.writeChunk(chunkPath, inputStream, length).whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Failed to write data to s3, position: {}, length: {}", position, length, throwable);
                TieredStoreException exception = new TieredStoreException(TieredStoreErrorCode.IO_ERROR, "write data to s3 error");
                exception.setPosition(position);
                completableFuture.completeExceptionally(exception);
            } else {
                if (result) {
                    ChunkMetadata chunk = new ChunkMetadata(chunkPath, position, length);
                    if (!this.metadata.addChunk(chunk)) {
                        // the chunk is not valid
                        LOGGER.error("The chunk: {} is not valid, now chunks last end position: {}, please check it.", chunk, this.metadata.getEndPosition());
                        throw new RuntimeException("The chunk: " + chunk + " is not valid, now chunks last end position: " + this.metadata.getEndPosition() + ", please check it.");
                    }
                    completableFuture.complete(true);
                } else {
                    completableFuture.complete(false);
                }
            }
        });
        return completableFuture;
    }


    public S3FileSegmentMetadata getMetadata() {
        return metadata;
    }

    public String getStorePath() {
        return storePath;
    }

    public TieredStorageS3Client getClient() {
        return client;
    }

    static class ConcurrentByteBuffer {
        private final ByteBuffer byteBuffer;
        private final int length;

        private final ReentrantLock reentrantLock;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        public ConcurrentByteBuffer(int length) {
            this.length = length;
            this.byteBuffer = ByteBuffer.allocate(length);
            this.byteBuffer.limit(this.length);
            this.reentrantLock = new ReentrantLock();
        }

        public void put(byte[] bytes, int bytesIndex, int writeLength, int writePosition) throws Exception {
            if (closed.get()) {
                throw new RuntimeException("The ConcurrentByteBuffer has been closed");
            }
            this.reentrantLock.lock();
            try {
                this.byteBuffer.position(writePosition);
                this.byteBuffer.put(bytes, bytesIndex, writeLength);
            } catch (Exception e) {
                LOGGER.error("Put bytes into byte buffer error. bytesIndex: {}, writeLength: {}, writePosition: {}", bytesIndex, writeLength, writePosition, e);
                throw e;
            } finally {
                this.reentrantLock.unlock();
            }
        }

        public ByteBuffer close() {
            this.closed.set(true);
            this.reentrantLock.lock();
            try {
                this.byteBuffer.rewind();
                return this.byteBuffer;
            } finally {
                this.reentrantLock.unlock();
            }
        }
    }

}
