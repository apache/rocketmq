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

import com.adobe.testing.s3mock.junit4.S3MockRule;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.junit.ClassRule;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class MockS3AsyncClient implements S3AsyncClient {

    @ClassRule
    public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

    public static TieredStorageS3Client getMockTieredStorageS3Client(TieredMessageStoreConfig config, S3MockStarterTestImpl s3MockApplication) {
        TieredStorageS3Client tieredStorageS3Client = null;
        try {
            tieredStorageS3Client = new TieredStorageS3Client(config);
            S3Client s3Client = s3MockApplication.createS3ClientV2();
            S3AsyncClient asyncClient = new MockS3AsyncClient(s3Client);
            Field clientField = tieredStorageS3Client.getClass().getDeclaredField("client");
            clientField.setAccessible(true);
            clientField.set(tieredStorageS3Client, asyncClient);
            s3Client.createBucket(CreateBucketRequest.builder().bucket(config.getS3Bucket()).build());
        } catch (Exception ignore) {

        }
        return tieredStorageS3Client;
    }

    private final S3Client s3Client;

    public MockS3AsyncClient(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {
        this.s3Client.close();
    }

    @Override
    public CompletableFuture<CreateBucketResponse> createBucket(CreateBucketRequest createBucketRequest) {
        return CompletableFuture.completedFuture(this.s3Client.createBucket(createBucketRequest));
    }

    @Override
    public CompletableFuture<PutObjectResponse> putObject(PutObjectRequest putObjectRequest, AsyncRequestBody requestBody) {
        List<ByteBuffer> list = new LinkedList<>();
        CompletableFuture<Void> future = requestBody.subscribe(bytebuffer -> {
            list.add(bytebuffer);
        });
        future.join();
        Integer len = list.stream().map(a -> a.limit()).reduce((a, b) -> a + b).get();
        ByteBuffer realByteBuffer = ByteBuffer.allocate(len);
        for (int i = 0; i < list.size(); i++) {
            ByteBuffer byteBuffer = list.get(i);
            byteBuffer.rewind();
            realByteBuffer.put(byteBuffer);
        }
        realByteBuffer.flip();
        RequestBody body = RequestBody.fromByteBuffer(realByteBuffer);
        return CompletableFuture.completedFuture(this.s3Client.putObject(putObjectRequest, body));
    }

    @Override
    public CompletableFuture<ListObjectsV2Response> listObjectsV2(Consumer<ListObjectsV2Request.Builder> listObjectsV2Request) {
        ListObjectsV2Request request = ListObjectsV2Request.builder().applyMutation(listObjectsV2Request).build();
        return this.listObjectsV2(request);
    }

    @Override
    public CompletableFuture<ListObjectsV2Response> listObjectsV2(ListObjectsV2Request listObjectsV2Request) {
        return CompletableFuture.completedFuture(this.s3Client.listObjectsV2(listObjectsV2Request));
    }

    @Override
    public CompletableFuture<DeleteObjectResponse> deleteObject(Consumer<DeleteObjectRequest.Builder> deleteObjectRequest) {
        DeleteObjectRequest request = DeleteObjectRequest.builder().applyMutation(deleteObjectRequest).build();
        return this.deleteObject(request);
    }

    @Override
    public CompletableFuture<DeleteObjectResponse> deleteObject(DeleteObjectRequest deleteObjectRequest) {
        return CompletableFuture.completedFuture(this.s3Client.deleteObject(deleteObjectRequest));
    }

    @Override
    public CompletableFuture<DeleteObjectsResponse> deleteObjects(Consumer<DeleteObjectsRequest.Builder> deleteObjectsRequest) {
        DeleteObjectsRequest request = DeleteObjectsRequest.builder().applyMutation(deleteObjectsRequest).build();
        return this.deleteObjects(request);
    }

    @Override
    public CompletableFuture<DeleteObjectsResponse> deleteObjects(DeleteObjectsRequest deleteObjectsRequest) {
        return CompletableFuture.completedFuture(this.s3Client.deleteObjects(deleteObjectsRequest));
    }

    @Override
    public <T> CompletableFuture<T> getObject(Consumer<GetObjectRequest.Builder> getObjectRequest, AsyncResponseTransformer<GetObjectResponse, T> asyncResponseTransformer) {
        GetObjectRequest request = GetObjectRequest.builder().applyMutation(getObjectRequest).build();
        return this.getObject(request, asyncResponseTransformer);
    }

    @Override
    public <T> CompletableFuture<T> getObject(GetObjectRequest getObjectRequest, AsyncResponseTransformer<GetObjectResponse, T> asyncResponseTransformer) {
        ResponseBytes<GetObjectResponse> resp = this.s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes());
        return CompletableFuture.completedFuture((T) resp);
    }

    @Override
    public CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload(Consumer<CreateMultipartUploadRequest.Builder> createMultipartUploadRequest) {
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder().applyMutation(createMultipartUploadRequest).build();
        return this.createMultipartUpload(request);
    }

    @Override
    public CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload(CreateMultipartUploadRequest createMultipartUploadRequest) {
        return CompletableFuture.completedFuture(this.s3Client.createMultipartUpload(createMultipartUploadRequest));
    }

    @Override
    public CompletableFuture<UploadPartCopyResponse> uploadPartCopy(Consumer<UploadPartCopyRequest.Builder> uploadPartCopyRequest) {
        UploadPartCopyRequest request = UploadPartCopyRequest.builder().applyMutation(uploadPartCopyRequest).build();
        return this.uploadPartCopy(request);
    }

    @Override
    public CompletableFuture<UploadPartCopyResponse> uploadPartCopy(UploadPartCopyRequest uploadPartCopyRequest) {
        return CompletableFuture.completedFuture(this.s3Client.uploadPartCopy(uploadPartCopyRequest));
    }

    @Override
    public CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(Consumer<CompleteMultipartUploadRequest.Builder> completeMultipartUploadRequest) {
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder().applyMutation(completeMultipartUploadRequest).build();
        return this.completeMultipartUpload(request);
    }

    @Override
    public CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) {
        return CompletableFuture.completedFuture(this.s3Client.completeMultipartUpload(completeMultipartUploadRequest));
    }

    @Override
    public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUpload(Consumer<AbortMultipartUploadRequest.Builder> abortMultipartUploadRequest) {
        AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder().applyMutation(abortMultipartUploadRequest).build();
        return S3AsyncClient.super.abortMultipartUpload(request);
    }

    @Override
    public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest) {
        return CompletableFuture.completedFuture(this.s3Client.abortMultipartUpload(abortMultipartUploadRequest));
    }
}
