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

import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.tieredstore.util.TieredStoreUtil.MB;

public class TieredStorageS3ClientTest extends MockS3TestBase {

    private static final TieredMessageStoreConfig CONFIG = new TieredMessageStoreConfig();

    private static final String BASE_DIR = "123/c/b/t/0/CommitLog/seg-0";

    static {
        CONFIG.setBrokerClusterName("test-cluster");
        CONFIG.setBrokerName("test-broker");
        CONFIG.setObjectStoreRegion("ap-northeast-1");
        CONFIG.setObjectStoreBucket("rocketmq-lcy");
        CONFIG.setObjectStoreAccessKey("");
        CONFIG.setObjectStoreSecretKey("");
    }

    private TieredStorageS3Client client;

    @Before
    public void setUp() {
        startMockedS3();
        client = MockS3AsyncClient.getMockTieredStorageS3Client(CONFIG, s3MockStater);
    }

    @After
    public void tearDown() {
        clearMockS3Data();
    }

    @Test
    public void testWriteChunk() {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        String chunkName = BASE_DIR + File.separator + "chunk-0";
        CompletableFuture<Boolean> completableFuture = client.writeChunk(chunkName, inputStream, 4);
        Assert.assertTrue(completableFuture.join());
    }

    @Test
    public void testReadChunk() {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        String chunkName = BASE_DIR + File.separator + "chunk-0";
        CompletableFuture<Boolean> completableFuture = client.writeChunk(chunkName, inputStream, 4);
        Assert.assertTrue(completableFuture.join());
        byte[] bytes = client.readChunk(chunkName, 0, 4).join();
        Assert.assertEquals("test", new String(bytes));
    }

    @Test
    public void testListChunks() {
        for (int i = 0; i < 10; i++) {
            String chunkName = BASE_DIR + File.separator + "chunk-" + (i * 5);
            InputStream inputStream = new ByteArrayInputStream(("test" + i).getBytes());
            CompletableFuture<Boolean> completableFuture = client.writeChunk(chunkName, inputStream, 5);
            Assert.assertTrue(completableFuture.join());
        }
        List<ChunkMetadata> chunks = client.listChunks(BASE_DIR).join();
        Assert.assertEquals(10, chunks.size());
        for (int i = 0; i < 10; i++) {
            ChunkMetadata chunkMetadata = chunks.get(i);
            String chunkName = BASE_DIR + File.separator + "chunk-" + (i * 5);
            Assert.assertEquals(chunkName, chunkMetadata.getChunkName());
            Assert.assertEquals(i * 5, chunkMetadata.getStartPosition());
            Assert.assertEquals(5, chunkMetadata.getChunkSize());
        }
    }

    @Test
    public void testExist() {
        String chunkName = BASE_DIR + File.separator + "chunk-0";
        Assert.assertFalse(client.exist(chunkName).join());

        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        CompletableFuture<Boolean> completableFuture = client.writeChunk(chunkName, inputStream, 4);
        Assert.assertTrue(completableFuture.join());

        Assert.assertTrue(client.exist(chunkName).join());
    }

    @Test
    public void testDeleteObjects() {
        for (int i = 0; i < 10; i++) {
            String chunkName = BASE_DIR + File.separator + "chunk-" + (i * 5);
            InputStream inputStream = new ByteArrayInputStream(("test" + i).getBytes());
            CompletableFuture<Boolean> completableFuture = client.writeChunk(chunkName, inputStream, 5);
            Assert.assertTrue(completableFuture.join());
        }
        List<ChunkMetadata> chunks = client.listChunks(BASE_DIR).join();
        Assert.assertEquals(10, chunks.size());
        for (int i = 0; i < 10; i++) {
            ChunkMetadata chunkMetadata = chunks.get(i);
            String chunkName = BASE_DIR + File.separator + "chunk-" + (i * 5);
            Assert.assertEquals(chunkName, chunkMetadata.getChunkName());
            Assert.assertEquals(i * 5, chunkMetadata.getStartPosition());
            Assert.assertEquals(5, chunkMetadata.getChunkSize());
        }

        List<String> undeleted = client.deleteObjects(BASE_DIR).join();
        Assert.assertTrue(undeleted.isEmpty());

        chunks = client.listChunks(BASE_DIR).join();
        Assert.assertEquals(0, chunks.size());
    }

    @Test
    public void testMergeAllChunksIntoSegment() {
        int unit = (int) (5 * MB);
        List<ChunkMetadata> chunks = new ArrayList<>(2);
        ByteBuffer byteBuffer = ByteBuffer.allocate(unit);
        for (int i = 0; i < unit; i++) {
            byteBuffer.put((byte) i);
        }
        byte[] bytes = byteBuffer.array();
        for (int i = 0; i < 2; i++) {
            String chunkName = BASE_DIR + File.separator + "chunk-" + (i * unit);
            chunks.add(new ChunkMetadata(chunkName, i * unit, unit));
            InputStream inputStream = new ByteArrayInputStream(bytes);
            CompletableFuture<Boolean> completableFuture = client.writeChunk(chunkName, inputStream, unit);
            Assert.assertTrue(completableFuture.join());
        }
        String segName = BASE_DIR + File.separator + "segment-0";
        Boolean merged = this.client.mergeAllChunksIntoSegment(chunks, segName).join();
        Assert.assertTrue(merged);
        byte[] segBytes = this.client.readChunk(segName, 0, 2 * unit).join();
        Assert.assertEquals(2 * unit, segBytes.length);
        for (int i = 0; i < 2; i++) {
            int offset = i * unit;
            for (int j = 0; j < unit; j++) {
                Assert.assertEquals(bytes[j], segBytes[j + offset]);
            }
        }
    }

}
