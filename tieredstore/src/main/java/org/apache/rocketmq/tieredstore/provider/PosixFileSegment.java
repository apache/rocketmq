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
package org.apache.rocketmq.tieredstore.provider;

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
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_FILE_TYPE;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_OPERATION;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_PATH;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_SUCCESS;

/**
 * this class is experimental and may change without notice.
 */
public class PosixFileSegment extends FileSegment {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static final String OPERATION_POSIX_READ = "read";
    private static final String OPERATION_POSIX_WRITE = "write";

    private final String fullPath;
    private volatile File file;
    private volatile FileChannel readFileChannel;
    private volatile FileChannel writeFileChannel;

    public PosixFileSegment(MessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {

        super(storeConfig, fileType, filePath, baseOffset);

        // basePath
        String basePath = StringUtils.defaultString(storeConfig.getTieredStoreFilePath(),
            StringUtils.appendIfMissing(storeConfig.getTieredStoreFilePath(), File.separator));

        // fullPath: basePath/hash_cluster/broker/topic/queueId/fileType/baseOffset
        String clusterName = storeConfig.getBrokerClusterName();
        String clusterBasePath = String.format("%s_%s", MessageStoreUtil.getHash(clusterName), clusterName);
        fullPath = Paths.get(basePath, clusterBasePath, filePath,
            fileType.toString(), MessageStoreUtil.offset2FileName(baseOffset)).toString();
        log.info("Constructing Posix FileSegment, filePath: {}", fullPath);

        this.createFile();
    }

    protected AttributesBuilder newAttributesBuilder() {
        return TieredStoreMetricsManager.newAttributesBuilder()
            .put(LABEL_PATH, filePath)
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
        return 0L;
    }

    @Override
    public boolean exists() {
        return file != null && file.exists();
    }

    @Override
    public void createFile() {
        if (this.file == null) {
            synchronized (this) {
                if (this.file == null) {
                    this.createFile0();
                }
            }
        }
    }

    @SuppressWarnings({"resource", "ResultOfMethodCallIgnored"})
    private void createFile0() {
        try {
            File file = new File(fullPath);
            File dir = file.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
            if (!file.exists()) {
                if (file.createNewFile()) {
                    log.debug("Create Posix FileSegment, filePath: {}", fullPath);
                }
            }
            this.readFileChannel = new RandomAccessFile(file, "r").getChannel();
            this.writeFileChannel = new RandomAccessFile(file, "rwd").getChannel();
            this.file = file;
        } catch (Exception e) {
            log.error("PosixFileSegment#createFile: create file {} failed: ", filePath, e);
        }
    }

    @Override

    public void destroyFile() {
        this.close();
        if (file != null && file.exists()) {
            if (file.delete()) {
                log.info("Destroy Posix FileSegment, filePath: {}", fullPath);
            } else {
                log.warn("Destroy Posix FileSegment error, filePath: {}", fullPath);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            if (readFileChannel != null && readFileChannel.isOpen()) {
                readFileChannel.close();
                readFileChannel = null;
            }
            if (writeFileChannel != null && writeFileChannel.isOpen()) {
                writeFileChannel.close();
                writeFileChannel = null;
            }
        } catch (IOException e) {
            log.error("Destroy Posix FileSegment failed, filePath: {}", fullPath, e);
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
            byteBuffer.limit(length);

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
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public CompletableFuture<Boolean> commit0(
        FileSegmentInputStream inputStream, long position, int length, boolean append) {

        Stopwatch stopwatch = Stopwatch.createStarted();
        AttributesBuilder attributesBuilder = newAttributesBuilder()
            .put(LABEL_OPERATION, OPERATION_POSIX_WRITE);

        return CompletableFuture.supplyAsync(() -> {
            try {
                byte[] byteArray = ByteStreams.toByteArray(inputStream);
                writeFileChannel.position(position);
                ByteBuffer buffer = ByteBuffer.wrap(byteArray);
                while (buffer.hasRemaining()) {
                    writeFileChannel.write(buffer);
                }
                writeFileChannel.force(true);
                attributesBuilder.put(LABEL_SUCCESS, true);
                long costTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                TieredStoreMetricsManager.providerRpcLatency.record(costTime, attributesBuilder.build());

                Attributes metricsAttributes = newAttributesBuilder()
                    .put(LABEL_OPERATION, OPERATION_POSIX_WRITE)
                    .build();
                TieredStoreMetricsManager.uploadBytes.record(length, metricsAttributes);
            } catch (Exception e) {
                long costTime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
                attributesBuilder.put(LABEL_SUCCESS, false);
                TieredStoreMetricsManager.providerRpcLatency.record(costTime, attributesBuilder.build());
                return false;
            }
            return true;
        }, MessageStoreExecutor.getInstance().bufferCommitExecutor);
    }
}
