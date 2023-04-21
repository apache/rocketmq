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

import com.adobe.testing.s3mock.S3MockApplication;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.provider.MockTieredFileSegmentInputStream;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.apache.rocketmq.tieredstore.util.TieredStoreUtil.MB;

public class S3FileSegmentTest extends MockS3TestBase {

    private static final TieredMessageStoreConfig CONFIG = new TieredMessageStoreConfig();

    static {
        CONFIG.setBrokerClusterName("test-cluster");
        CONFIG.setBrokerName("test-broker");
        CONFIG.setObjectStoreRegion("ap-northeast-1");
        CONFIG.setObjectStoreBucket("rocketmq-lcy");
        CONFIG.setObjectStoreAccessKey("");
        CONFIG.setObjectStoreSecretKey("");
    }

    private static final Map<String, Object> PROPERTIES = new HashMap<String, Object>();

    static {
        PROPERTIES.put(S3MockApplication.PROP_HTTP_PORT, S3MockApplication.RANDOM_PORT);
        PROPERTIES.put(S3MockApplication.PROP_HTTPS_PORT, S3MockApplication.RANDOM_PORT);
        PROPERTIES.put(S3MockApplication.PROP_INITIAL_BUCKETS, CONFIG.getObjectStoreBucket());
    }

    private static final MessageQueue MQ = new MessageQueue();

    static {
        MQ.setBrokerName("test-broker");
        MQ.setQueueId(0);
        MQ.setTopic("test-topic");
    }

    private static final long BASE_OFFSET = 1024;

    private static final TieredFileSegment.FileSegmentType TYPE = TieredFileSegment.FileSegmentType.CONSUME_QUEUE;

    private S3FileSegment segment;

    @Before
    public void setUp() {
        startMockedS3();
        segment = new S3FileSegment(TYPE, MQ, BASE_OFFSET, CONFIG);
    }

    @After
    public void tearDown() {
        clearMockS3Data();
    }

    @Test
    public void testNewInstance() {
        S3FileSegmentMetadata metadata = segment.getMetadata();
        Assert.assertEquals(0, metadata.getSize());
    }

    @Test
    public void testCommit() {
        InputStream inputStream = new ByteArrayInputStream("hello".getBytes());
        segment.commit0(new MockTieredFileSegmentInputStream(inputStream), 0, 5, false).join();
        ByteBuffer read = segment.read0(0, 5).join();
        Assert.assertEquals("hello", new String(read.array()));
        Assert.assertEquals(5, segment.getSize());
        Assert.assertEquals(0, segment.getMetadata().getStartPosition());
        Assert.assertEquals(4, segment.getMetadata().getEndPosition());
    }

    @Test
    public void testCommitAndRestart() {
        InputStream inputStream = new ByteArrayInputStream("hello".getBytes());
        segment.commit0(new MockTieredFileSegmentInputStream(inputStream), 0, 5, false).join();
        ByteBuffer read = segment.read0(0, 5).join();
        Assert.assertEquals("hello", new String(read.array()));
        Assert.assertEquals(5, segment.getSize());
        Assert.assertEquals(0, segment.getMetadata().getStartPosition());
        Assert.assertEquals(4, segment.getMetadata().getEndPosition());

        segment = new S3FileSegment(TYPE, MQ, BASE_OFFSET, CONFIG);
        Assert.assertEquals(5, segment.getSize());
        Assert.assertEquals(0, segment.getMetadata().getStartPosition());
        Assert.assertEquals(4, segment.getMetadata().getEndPosition());
        read = segment.read0(0, 5).join();
        Assert.assertEquals("hello", new String(read.array()));
    }

    @Test
    public void testRestartWithInvalidChunks() {
        // write invalid chunks
        TieredStorageS3Client client = TieredStorageS3Client.getInstance(CONFIG);
        client.writeChunk(segment.getChunkPath() + File.separator + "chunk-" + 0, new ByteArrayInputStream("hello".getBytes()), 5).join();
        client.writeChunk(segment.getChunkPath() + File.separator + "chunk-" + 1, new ByteArrayInputStream("world".getBytes()), 5).join();

        // initialize invalid chunks
        Assert.assertThrows(RuntimeException.class, () -> segment = new S3FileSegment(TYPE, MQ, BASE_OFFSET, CONFIG));
    }

    @Test
    public void testRestartWithInvalidSegments() {
        // write two segments
        TieredStorageS3Client client = TieredStorageS3Client.getInstance(CONFIG);
        client.writeChunk(segment.getSegmentPath() + File.separator + "segment-" + 0, new ByteArrayInputStream("hello".getBytes()), 5).join();
        client.writeChunk(segment.getSegmentPath() + File.separator + "segment-" + 1, new ByteArrayInputStream("world".getBytes()), 5).join();

        // initialize invalid segments
        Assert.assertThrows(RuntimeException.class, () -> segment = new S3FileSegment(TYPE, MQ, BASE_OFFSET, CONFIG));
    }

    @Test
    public void testCommitAndDelete() {
        InputStream inputStream = new ByteArrayInputStream("hello".getBytes());
        segment.commit0(new MockTieredFileSegmentInputStream(inputStream), 0, 5, false).join();
        ByteBuffer read = segment.read0(0, 5).join();
        Assert.assertEquals("hello", new String(read.array()));
        segment.destroyFile();
        segment = new S3FileSegment(TYPE, MQ, BASE_OFFSET, CONFIG);
        Assert.assertEquals(0, segment.getSize());
        Assert.assertEquals(-1, segment.getMetadata().getStartPosition());
        Assert.assertEquals(-1, segment.getMetadata().getEndPosition());
        Assert.assertTrue(segment.read0(0, 5).isCompletedExceptionally());
    }

    @Test
    public void testBackwardCommitPosition() {
        // write first chunk: "hello", size = 5, position: [0, 4]
        InputStream inputStream = new ByteArrayInputStream("hello".getBytes());
        Assert.assertTrue(segment.commit0(new MockTieredFileSegmentInputStream(inputStream), 0, 5, false).join());
        ByteBuffer read = segment.read0(0, 5).join();
        Assert.assertEquals("hello", new String(read.array()));
        // write second chunk: ",world", size = 6, position: [5, 10]
        inputStream = new ByteArrayInputStream(",world".getBytes());
        Assert.assertTrue(segment.commit0(new MockTieredFileSegmentInputStream(inputStream), 5, 6, false).join());
        read = segment.read0(0, 11).join();
        Assert.assertEquals("hello,world", new String(read.array()));
        // write third chunk: " and lcy", size = 8, position: [11, 18]
        inputStream = new ByteArrayInputStream(" and lcy".getBytes());
        Assert.assertTrue(segment.commit0(new MockTieredFileSegmentInputStream(inputStream), 11, 8, false).join());
        read = segment.read0(0, 19).join();
        Assert.assertEquals("hello,world and lcy", new String(read.array()));
        // write a chunk from position 2, size = 2, data: "he", position: [2, 3]
        // this chunk should truncate the first chunk and delete all chunks after the first chunk
        inputStream = new ByteArrayInputStream("he".getBytes());
        TieredStoreException exception = null;
        try {
            segment.commit0(new MockTieredFileSegmentInputStream(inputStream), 2, 2, false).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof TieredStoreException);
            exception = (TieredStoreException) cause;
            Assert.assertEquals(TieredStoreErrorCode.ILLEGAL_OFFSET, exception.getErrorCode());
            Assert.assertEquals(18, exception.getPosition());
        }
        Assert.assertNotNull(exception);
    }
    @Test
    public void testSeal() throws Exception {
        int unit = (int) (5 * MB);
        ByteBuffer byteBuffer = ByteBuffer.allocate(unit);
        for (int i = 0; i < unit; i++) {
            byteBuffer.put((byte) i);
        }
        byte[] array = byteBuffer.array();
        for (int i = 0; i < 2; i++) {
            segment.commit0(new MockTieredFileSegmentInputStream(new ByteArrayInputStream(array)), i * unit, unit, false).join();
        }
        // seal
        segment.sealFile();
        Thread.sleep(3000);

        Assert.assertTrue(segment.isSealed());
        S3FileSegmentMetadata metadata = segment.getMetadata();
        Assert.assertEquals(0, metadata.getChunkCount());
        Assert.assertEquals(0, metadata.getStartPosition());
        Assert.assertEquals(2 * unit - 1, metadata.getEndPosition());
        Assert.assertEquals(2 * unit, metadata.getSize());
        TieredStoreException exception = null;
        try {
            segment.commit0(new MockTieredFileSegmentInputStream(new ByteArrayInputStream(new byte[]{0, 1, 2})), 2 * unit, 3, false).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof TieredStoreException);
            exception = (TieredStoreException) cause;
            Assert.assertEquals(TieredStoreErrorCode.SEGMENT_SEALED, exception.getErrorCode());
        }
        Assert.assertNotNull(exception);
    }

}
