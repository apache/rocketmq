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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_OPERATION;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_SUCCESS;

public class TieredStorageS3Client {

    private static final String OPERATION_LIST_OBJECTS = "list_objects";

    private static final String OPERATION_DELETE_OBJECTS = "delete_objects";

    private static final String OPERATION_UPLOAD_OBJECT = "upload_object";

    private static final String OPERATION_DOWNLOAD_OBJECT = "download_object";

    private static final Logger LOGGER = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);
    private volatile static TieredStorageS3Client instance;

    private final String region;

    private final String bucket;

    private final TieredMessageStoreConfig tieredMessageStoreConfig;

    private final ExecutorService asyncRequestBodyExecutor;

    private S3AsyncClient client;

    public static TieredStorageS3Client getInstance(TieredMessageStoreConfig config) {
        if (config == null) {
            return instance;
        }
        if (instance == null) {
            synchronized (TieredStorageS3Client.class) {
                if (instance == null) {
                    instance = new TieredStorageS3Client(config, true);
                }
            }
        }
        return instance;
    }

    @VisibleForTesting
    protected TieredStorageS3Client(TieredMessageStoreConfig config) {
        this(config, false);
    }

    private TieredStorageS3Client(TieredMessageStoreConfig config, boolean createClient) {
        this.tieredMessageStoreConfig = config;
        this.region = config.getObjectStoreRegion();
        this.bucket = config.getObjectStoreBucket();
        if (createClient) {
            AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(this.tieredMessageStoreConfig.getObjectStoreAccessKey(), this.tieredMessageStoreConfig.getObjectStoreSecretKey());
            this.client = S3AsyncClient.builder().credentialsProvider(() -> basicCredentials).region(Region.of(config.getObjectStoreRegion())).build();
        }
        this.asyncRequestBodyExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("S3AsyncRequestBodyExecutor_"));
    }

    public CompletableFuture<Boolean> writeChunk(String key, InputStream inputStream, long length) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(this.bucket).key(key).build();
        AsyncRequestBody requestBody = AsyncRequestBody.fromInputStream(inputStream, length, this.asyncRequestBodyExecutor);
        AttributesBuilder attributesBuilder = TieredStoreMetricsManager.newAttributesBuilder().put(LABEL_OPERATION, OPERATION_UPLOAD_OBJECT);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<PutObjectResponse> putObjectResponseCompletableFuture = this.client.putObject(putObjectRequest, requestBody);
        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
        putObjectResponseCompletableFuture.whenComplete((putObjectResponse, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Upload file to S3 failed, key: {}, region: {}, bucket: {}", key, this.region, this.bucket, throwable);
                attributesBuilder.put(LABEL_SUCCESS, false);
                completableFuture.complete(false);
            } else {
                attributesBuilder.put(LABEL_SUCCESS, true);
                completableFuture.complete(true);
            }
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        });
        return completableFuture;
    }

    public CompletableFuture<List<ChunkMetadata>> listChunks(String prefix) {
        CompletableFuture<List<ChunkMetadata>> completableFuture = new CompletableFuture<>();
        AttributesBuilder attributesBuilder = TieredStoreMetricsManager.newAttributesBuilder().put(LABEL_OPERATION, OPERATION_LIST_OBJECTS);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<ListObjectsV2Response> listFuture = this.listObjects(prefix);
        listFuture.whenComplete((listObjectsV2Response, throwable) -> {
            if (throwable != null) {
                attributesBuilder.put(LABEL_SUCCESS, false);
                LOGGER.error("List objects from S3 failed, prefix: {}, region: {}, bucket: {}", prefix, this.region, this.bucket, throwable);
                completableFuture.complete(Collections.emptyList());
            } else {
                attributesBuilder.put(LABEL_SUCCESS, true);
                listObjectsV2Response.contents().forEach(s3Object -> LOGGER.info("List objects from S3, key: {}, region: {}, bucket: {}", s3Object.key(), this.region, this.bucket));
                completableFuture.complete(listObjectsV2Response.contents().stream().map(obj -> {
                    ChunkMetadata chunkMetadata = new ChunkMetadata();
                    String key = obj.key();
                    chunkMetadata.setChunkName(key);
                    chunkMetadata.setChunkSize(obj.size().intValue());
                    String[] paths = key.split("/");
                    String chunkSubName = paths[paths.length - 1];
                    Integer startPosition = Integer.valueOf(chunkSubName.split("-")[1]);
                    chunkMetadata.setStartPosition(startPosition);
                    return chunkMetadata;
                }).sorted((o1, o2) -> (int) (o1.getStartPosition() - o2.getStartPosition())).collect(Collectors.toList()));
            }
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        });
        return completableFuture;
    }

    public CompletableFuture<ListObjectsV2Response> listObjects(String prefix) {
        AttributesBuilder attributesBuilder = TieredStoreMetricsManager.newAttributesBuilder().put(LABEL_OPERATION, OPERATION_LIST_OBJECTS);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<ListObjectsV2Response> listFuture = this.client.listObjectsV2(builder -> builder.bucket(this.bucket).prefix(prefix));
        return listFuture.thenApply(resp -> {
            attributesBuilder.put(LABEL_SUCCESS, true);
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            return resp;
        }).exceptionally(throwable -> {
            attributesBuilder.put(LABEL_SUCCESS, false);
            LOGGER.error("List objects from S3 failed, prefix: {}, region: {}, bucket: {}", prefix, this.region, this.bucket, throwable);
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            return null;
        });
    }

    public CompletableFuture<Boolean> exist(String prefix) {
        AttributesBuilder attributesBuilder = TieredStoreMetricsManager.newAttributesBuilder().put(LABEL_OPERATION, OPERATION_LIST_OBJECTS);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<ListObjectsV2Response> listFuture = this.listObjects(prefix);
        return listFuture.thenApply(resp -> {
            attributesBuilder.put(LABEL_SUCCESS, true);
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            return resp.contents().size() > 0;
        }).exceptionally(throwable -> {
            attributesBuilder.put(LABEL_SUCCESS, false);
            LOGGER.error("Exist prefix failed, list objects from S3 failed, prefix: {}, region: {}, bucket: {}", prefix, this.region, this.bucket, throwable);
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            return false;
        });
    }

    public CompletableFuture<List<String/*undeleted keys*/>> deleteObjets(final List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        List<ObjectIdentifier> objects = keys.stream().map(key -> ObjectIdentifier.builder().key(key).build()).collect(Collectors.toList());
        Delete delete = Delete.builder().objects(objects).build();
        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder().bucket(this.bucket).delete(delete).build();
        AttributesBuilder attributesBuilder = TieredStoreMetricsManager.newAttributesBuilder().put(LABEL_OPERATION, OPERATION_DELETE_OBJECTS);
        Stopwatch stopwatch = Stopwatch.createStarted();
        return this.client.deleteObjects(deleteObjectsRequest).thenApply(resp -> {
            attributesBuilder.put(LABEL_SUCCESS, true);
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            List<String> undeletedKeys;
            if (resp.deleted().size() != keys.size()) {
                List<String> deleted = resp.deleted().stream().map(DeletedObject::key).collect(Collectors.toList());
                undeletedKeys = keys.stream().filter(key -> !deleted.contains(key)).collect(Collectors.toList());
            } else {
                undeletedKeys = Collections.emptyList();
            }
            return undeletedKeys;
        }).exceptionally(throwable -> {
            LOGGER.error("Delete objects from S3 failed, keys: {}, region: {}, bucket: {}", keys, this.region, this.bucket, throwable);
            attributesBuilder.put(LABEL_SUCCESS, false);
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            return keys;
        });
    }

    public CompletableFuture<List<String>> deleteObjects(String prefix) {
        CompletableFuture<List<String>> readObjectsByPrefix = this.listObjects(prefix).
            thenApply(resp -> resp.contents().stream().map(S3Object::key).collect(Collectors.toList()));
        return readObjectsByPrefix.thenCompose(this::deleteObjets);
    }

    public CompletableFuture<byte[]> readChunk(String key, long startPosition, long endPosition) {
        GetObjectRequest request = GetObjectRequest.builder().bucket(this.bucket).key(key).range("bytes=" + startPosition + "-" + endPosition).build();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        AttributesBuilder attributesBuilder = TieredStoreMetricsManager.newAttributesBuilder().put(LABEL_OPERATION, OPERATION_DOWNLOAD_OBJECT);
        Stopwatch stopwatch = Stopwatch.createStarted();
        this.client.getObject(request, AsyncResponseTransformer.toBytes()).whenComplete((response, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Read chunk from S3 failed, key: {}, region: {}, bucket: {}", key, this.region, this.bucket, throwable);
                attributesBuilder.put(LABEL_SUCCESS, false);
                future.completeExceptionally(throwable);
            } else {
                attributesBuilder.put(LABEL_SUCCESS, true);
                future.complete(response.asByteArray());
            }
            TieredStoreMetricsManager.providerRpcLatency.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        });
        return future;
    }

    public CompletableFuture<Boolean> mergeAllChunksIntoSegment(List<ChunkMetadata> chunks, String segmentName) {
        AsyncS3ChunksMerger merger = new AsyncS3ChunksMerger(segmentName, chunks);
        return merger.run();
    }

    class AsyncS3ChunksMerger {
        private final String segmentKey;
        private String uploadId;
        private final List<CompletedPart> completedParts;

        private final List<ChunkMetadata> chunks;

        public AsyncS3ChunksMerger(String segmentKey, List<ChunkMetadata> chunks) {
            this.segmentKey = segmentKey;
            this.uploadId = null;
            this.completedParts = new ArrayList<>();
            this.chunks = chunks;
        }

        public CompletableFuture<Boolean> run() {
            return initiateUpload().thenCompose(uploadId -> {
                List<CompletableFuture<CompletedPart>> uploadPartFutures = new ArrayList<>(chunks.size());
                for (int i = 0; i < chunks.size(); i++) {
                    String chunkKey = chunks.get(i).getChunkName();
                    int partNumber = i + 1;
                    uploadPartFutures.add(uploadPart(partNumber, chunkKey));
                }
                return CompletableFuture.allOf(uploadPartFutures.toArray(new CompletableFuture[0]));
            }).thenCompose(v -> completeUpload()).handle((resp, err) -> {
                if (err != null) {
                    LOGGER.error("Merge all chunks into segment failed, chunks: {}, segmentName: {}, region: {}, bucket: {}", chunks, segmentKey, region, bucket, err);
                    abortUpload().join();
                    return false;
                }
                return resp;
            });
        }

        private CompletableFuture<String> initiateUpload() {
            CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(segmentKey)
                .build();

            return client.createMultipartUpload(request)
                .thenApply(CreateMultipartUploadResponse::uploadId)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.error("Error initiating multi part upload: " + error);
                    } else {
                        uploadId = result;
                    }
                });
        }

        private CompletableFuture<CompletedPart> uploadPart(int partNumber, String chunkKey) {
            UploadPartCopyRequest request = UploadPartCopyRequest.builder()
                .sourceBucket(bucket).sourceKey(chunkKey).uploadId(uploadId).partNumber(partNumber)
                .destinationBucket(bucket).destinationKey(segmentKey)
                .build();

            return client.uploadPartCopy(request)
                .thenApply(resp -> resp.copyPartResult().eTag())
                .thenApply(eTag -> CompletedPart.builder().partNumber(partNumber).eTag(eTag).build())
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.error("Error uploading part, chunkKey: {}, partNumber: {}, uploadId: {}, error: {}", chunkKey, partNumber, uploadId, error);
                    } else {
                        completedParts.add(result);
                    }
                });
        }

        private CompletableFuture<Boolean> completeUpload() {
            Collections.sort(completedParts, Comparator.comparingInt(CompletedPart::partNumber));

            CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();

            CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(segmentKey)
                .uploadId(uploadId)
                .multipartUpload(multipartUpload)
                .build();

            return client.completeMultipartUpload(request)
                .thenApply(resp -> true)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        LOGGER.error("Error completing multi part upload, uploadId: {}, error: {}", uploadId, error);
                    }
                });
        }

        private CompletableFuture<Boolean> abortUpload() {
            AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(segmentKey)
                .uploadId(uploadId)
                .build();
            return client.abortMultipartUpload(request).thenApply(v -> true).exceptionally(e -> false);
        }
    }
}