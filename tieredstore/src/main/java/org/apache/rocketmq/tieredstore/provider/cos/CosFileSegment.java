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
package org.apache.rocketmq.tieredstore.provider.cos;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.AppendObjectRequest;
import com.qcloud.cos.model.AppendObjectResult;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.region.Region;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_FILE_TYPE;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_OPERATION;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_SUCCESS;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_TOPIC;

/**
 * this class is experimental and may change without notice.
 */
public class CosFileSegment extends TieredFileSegment {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static final String OPERATION_POSIX_READ = "read";
    private static final String OPERATION_POSIX_WRITE = "write";

    private final String basePath;
    private final String filepath;
    private String cosBucketName = "";
    private String cosAccessKey = "";
    private String cosSecretKey = "";
    private String cosRegion = "";
    private volatile boolean created;
    private COSClient cosClient;

    public CosFileSegment(FileSegmentType fileType, MessageQueue messageQueue,
        long baseOffset, TieredMessageStoreConfig storeConfig) {
        super(fileType, messageQueue, baseOffset, storeConfig);
        this.cosAccessKey = storeConfig.getCosAccessKey();
        this.cosSecretKey = storeConfig.getCosSecretKey();
        this.cosBucketName = storeConfig.getCosBucketName();
        this.cosRegion = storeConfig.getCosRegion();
        String basePath = storeConfig.getTieredStoreFilepath();
        if (StringUtils.isBlank(basePath) || basePath.endsWith(File.separator)) {
            this.basePath = basePath;
        } else {
            this.basePath = basePath + File.separator;
        }
        this.filepath = this.basePath
            + TieredStoreUtil.getHash(storeConfig.getBrokerClusterName()) + "_" + storeConfig.getBrokerClusterName() + File.separator
            + messageQueue.getBrokerName() + File.separator
            + messageQueue.getTopic() + File.separator
            + messageQueue.getQueueId() + File.separator
            + fileType + File.separator
            + TieredStoreUtil.offset2FileName(baseOffset);
        cosClient = buildCosClient();
        createFile();
    }

    protected AttributesBuilder newAttributesBuilder() {
        return TieredStoreMetricsManager.newAttributesBuilder()
            .put(LABEL_TOPIC, messageQueue.getTopic())
            .put(LABEL_FILE_TYPE, fileType.name().toLowerCase());
    }

    @Override
    public String getPath() {
        return filepath;
    }

    @Override
    public long getSize() {
        if (exists()) {
            ObjectMetadata meta = cosClient.getObjectMetadata(cosBucketName, filepath);
            return meta.getContentLength();
        }
        return -1;
    }

    @Override
    public boolean exists() {
        ObjectMetadata meta = cosClient.getObjectMetadata(cosBucketName, filepath);
        return meta != null;
    }

    @Override
    public void createFile() {
        if (!created) {
            synchronized (this) {
                if (!created) {
                    String bucketName = this.cosBucketName;
                    String key = filepath;
                    InputStream input = new ByteArrayInputStream(new byte[0]);
                    ObjectMetadata objectMetadata = new ObjectMetadata();
                    objectMetadata.setContentLength(0);
                    AppendObjectRequest putObjectRequest =
                        new AppendObjectRequest(bucketName, key, input, objectMetadata);
                    putObjectRequest.setPosition(0L);
                    AppendObjectResult putObjectResult = cosClient.appendObject(putObjectRequest);
                    created = true;
                }
            }
        }
    }

    @Override
    public void destroyFile() {
        if (exists()) {
            cosClient.deleteObject(cosBucketName, filepath);
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
            GetObjectRequest getObjectRequest = new GetObjectRequest(cosBucketName, filepath);
            COSObject cosObject = cosClient.getObject(getObjectRequest);
            int readed = cosObject.getObjectContent().read(byteBuffer.array(), (int) position, length);
            if (readed != length) {
                throw new IOException("read from cos failed,length is not matched");
            }
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
            logger.error("CosFileSegment#read0: read file {} failed: position: {}, length: {}",
                filepath, position, length, e);
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
                        logger.error("CosFileSegment#commit0: append file {} failed: real data size: {}, is not equal to length: {}",
                            filepath, byteArray.length, length);
                        future.complete(false);
                        return;
                    }

                    ByteBuf buffer = Unpooled.copiedBuffer(byteArray);
                    ByteBufInputStream backedInputStream = new ByteBufInputStream(buffer);
                    AppendObjectRequest appendObjectRequest = new AppendObjectRequest(this.cosBucketName, filepath, backedInputStream);
                    appendObjectRequest.setPosition(position);
                    cosClient.appendObject(appendObjectRequest);
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

                    logger.error("CosFileSegment#commit0: append file {} failed: position: {}, length: {}",
                        filepath, position, length, e);
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

    private COSClient buildCosClient() {
        COSCredentials cred = new BasicCOSCredentials(this.cosAccessKey, this.cosSecretKey);
        ClientConfig clientConfig = new ClientConfig(new Region(this.cosRegion));
        COSClient cosClient = new COSClient(cred, clientConfig);
        return cosClient;
    }
}
