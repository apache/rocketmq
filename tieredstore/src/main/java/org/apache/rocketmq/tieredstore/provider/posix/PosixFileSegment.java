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
package org.apache.rocketmq.tieredstore.provider.posix;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_FILE_TYPE;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_OPERATION;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_SUCCESS;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_TOPIC;

/**
 * this class is experimental and may change without notice.
 */
public class PosixFileSegment extends TieredFileSegment {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static final String UNDERLINE = "_";
    private static final String OPERATION_POSIX_READ = "read";
    private static final String OPERATION_POSIX_WRITE = "write";

    private volatile File file;
    private volatile FileChannel readFileChannel;
    private volatile FileChannel writeFileChannel;

    public PosixFileSegment(TieredMessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {

        super(storeConfig, fileType, filePath, baseOffset);

        // basePath
        String basePath = StringUtils.defaultString(storeConfig.getTieredStoreFilePath(),
            StringUtils.appendIfMissing(storeConfig.getTieredStoreFilePath(), File.separator));

        // fullPath: basePath/hash_cluster/broker/topic/queueId/fileType/baseOffset
        String brokerClusterName = storeConfig.getBrokerClusterName();
        String clusterBasePath = TieredStoreUtil.getHash(brokerClusterName) + UNDERLINE + brokerClusterName;
        String fullPath = Paths.get(basePath, clusterBasePath, filePath,
            fileType.toString(), TieredStoreUtil.offset2FileName(baseOffset)).toString();
        logger.info("Constructing Posix FileSegment, filePath: {}", fullPath);

        createFile();
    }

    protected AttributesBuilder newAttributesBuilder() {
        return TieredStoreMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, filePath)
            .put(LABEL_FILE_TYPE, fileType.name().toLowerCase());
    }

    @Override
    public String getPath() {
        return filePath;
    }

    @Override
    public long getSize() {
        if (exists()) {
            return file.length();
        }
        return -1;
    }

    @Override
    public boolean exists() {
        return file != null && file.exists();
    }

    @Override
    public void createFile() {
        if (file == null) {
            synchronized (this) {
                if (file == null) {
                    File file = new File(filePath);
                    try {
                        File dir = file.getParentFile();
                        if (!dir.exists()) {
                            dir.mkdirs();
                        }

                        // TODO use direct IO to avoid polluting the page cache
                        file.createNewFile();
                        this.readFileChannel = new RandomAccessFile(file, "r").getChannel();
                        this.writeFileChannel = new RandomAccessFile(file, "rwd").getChannel();
                        this.file = file;
                    } catch (Exception e) {
                        logger.error("PosixFileSegment#createFile: create file {} failed: ", filePath, e);
                    }
                }
            }
        }
    }

    @Override
    public void destroyFile() {
        try {
            if (readFileChannel != null && readFileChannel.isOpen()) {
                readFileChannel.close();
            }
            if (writeFileChannel != null && writeFileChannel.isOpen()) {
                writeFileChannel.close();
            }
        } catch (IOException e) {
            logger.error("PosixFileSegment#destroyFile: destroy file {} failed: ", filePath, e);
        }

        if (file.exists()) {
            file.delete();
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        AttributesBuilder attributesBuilder = newAttributesBuilder()
            .put(LABEL_OPERATION, OPERATION_POSIX_READ);

        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        try {
            readFileChannel.position(position);
            readFileChannel.read(byteBuffer);
            byteBuffer.flip();

            attributesBuilder.put(LABEL_SUCCESS, true);
            long costTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            TieredStoreMetricsManager.providerRpcLatency.record(costTime, attributesBuilder.build());

            Attributes metricsAttributes = newAttributesBuilder()
                .put(LABEL_OPERATION, OPERATION_POSIX_READ)
                .build();
            int downloadedBytes = byteBuffer.remaining();
            TieredStoreMetricsManager.downloadBytes.record(downloadedBytes, metricsAttributes);

            future.complete(byteBuffer);
        } catch (IOException e) {
            long costTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            attributesBuilder.put(LABEL_SUCCESS, false);
            TieredStoreMetricsManager.providerRpcLatency.record(costTime, attributesBuilder.build());
            logger.error("PosixFileSegment#read0: read file {} failed: position: {}, length: {}",
                filePath, position, length, e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Boolean> commit0(TieredFileSegmentInputStream inputStream, long position, int length,
                                              boolean append) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        AttributesBuilder attributesBuilder = newAttributesBuilder()
            .put(LABEL_OPERATION, OPERATION_POSIX_WRITE);

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            TieredStoreExecutor.commitExecutor.execute(() -> {
                try {
                    byte[] byteArray = ByteStreams.toByteArray(inputStream);
                    if (byteArray.length != length) {
                        logger.error("PosixFileSegment#commit0: append file {} failed: real data size: {}, is not equal to length: {}",
                            filePath, byteArray.length, length);
                        future.complete(false);
                        return;
                    }
                    writeFileChannel.position(position);
                    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
                    while (buffer.hasRemaining()) {
                        writeFileChannel.write(buffer);
                    }

                    attributesBuilder.put(LABEL_SUCCESS, true);
                    long costTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                    TieredStoreMetricsManager.providerRpcLatency.record(costTime, attributesBuilder.build());

                    Attributes metricsAttributes = newAttributesBuilder()
                        .put(LABEL_OPERATION, OPERATION_POSIX_WRITE)
                        .build();
                    TieredStoreMetricsManager.uploadBytes.record(length, metricsAttributes);

                    future.complete(true);
                } catch (Exception e) {
                    long costTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                    attributesBuilder.put(LABEL_SUCCESS, false);
                    TieredStoreMetricsManager.providerRpcLatency.record(costTime, attributesBuilder.build());

                    logger.error("PosixFileSegment#commit0: append file {} failed: position: {}, length: {}",
                        filePath, position, length, e);
                    future.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            // commit task cannot be executed
            long costTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            attributesBuilder.put(LABEL_SUCCESS, false);
            TieredStoreMetricsManager.providerRpcLatency.record(costTime, attributesBuilder.build());

            future.completeExceptionally(e);
        }
        return future;
    }
}
