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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;

public class FileSegmentTest {

    public int baseOffset = 1000;
    private final String storePath = MessageStoreUtilTest.getRandomStorePath();
    private MessageStoreConfig storeConfig;
    private MessageQueue mq;
    private MessageStoreExecutor storeExecutor;

    @Before
    public void init() {
        storeConfig = new MessageStoreConfig();
        storeConfig.setTieredStoreCommitLogMaxSize(2000);
        storeConfig.setTieredStoreFilePath(storePath);
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        mq = new MessageQueue("FileSegmentTest", "brokerName", 0);
        storeExecutor = new MessageStoreExecutor();
    }

    @After
    public void shutdown() {
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
        storeExecutor.shutdown();
    }

    @Test
    public void fileAttributesTest() {
        int baseOffset = 1000;
        FileSegment fileSegment = new PosixFileSegment(
            storeConfig, FileSegmentType.COMMIT_LOG, MessageStoreUtil.toFilePath(mq), baseOffset, storeExecutor);

        // for default value check
        Assert.assertEquals(baseOffset, fileSegment.getBaseOffset());
        Assert.assertEquals(0L, fileSegment.getCommitPosition());
        Assert.assertEquals(0L, fileSegment.getAppendPosition());
        Assert.assertEquals(baseOffset, fileSegment.getCommitOffset());
        Assert.assertEquals(baseOffset, fileSegment.getAppendOffset());
        Assert.assertEquals(FileSegmentType.COMMIT_LOG, fileSegment.getFileType());
        Assert.assertEquals(Long.MAX_VALUE, fileSegment.getMinTimestamp());
        Assert.assertEquals(Long.MAX_VALUE, fileSegment.getMaxTimestamp());

        // for recover
        long timestamp = System.currentTimeMillis();
        fileSegment.setMinTimestamp(timestamp);
        fileSegment.setMaxTimestamp(timestamp);
        Assert.assertEquals(timestamp, fileSegment.getMinTimestamp());
        Assert.assertEquals(timestamp, fileSegment.getMaxTimestamp());

        // for file status change
        Assert.assertFalse(fileSegment.isClosed());
        fileSegment.close();
        Assert.assertTrue(fileSegment.isClosed());

        fileSegment.destroyFile();
    }

    @Test
    public void fileSortByOffsetTest() {
        FileSegment fileSegment1 = new PosixFileSegment(
            storeConfig, FileSegmentType.COMMIT_LOG, MessageStoreUtil.toFilePath(mq), 200L, storeExecutor);
        FileSegment fileSegment2 = new PosixFileSegment(
            storeConfig, FileSegmentType.COMMIT_LOG, MessageStoreUtil.toFilePath(mq), 100L, storeExecutor);
        FileSegment[] fileSegments = new FileSegment[] {fileSegment1, fileSegment2};
        Arrays.sort(fileSegments);
        Assert.assertEquals(fileSegments[0], fileSegment2);
        Assert.assertEquals(fileSegments[1], fileSegment1);
    }

    @Test
    public void fileMaxSizeTest() {
        FileSegment fileSegment = new PosixFileSegment(
            storeConfig, FileSegmentType.COMMIT_LOG, MessageStoreUtil.toFilePath(mq), 100L, storeExecutor);
        Assert.assertEquals(storeConfig.getTieredStoreCommitLogMaxSize(), fileSegment.getMaxSize());
        fileSegment.destroyFile();

        fileSegment = new PosixFileSegment(
            storeConfig, FileSegmentType.CONSUME_QUEUE, MessageStoreUtil.toFilePath(mq), 100L, storeExecutor);
        Assert.assertEquals(storeConfig.getTieredStoreConsumeQueueMaxSize(), fileSegment.getMaxSize());
        fileSegment.destroyFile();

        fileSegment = new PosixFileSegment(
            storeConfig, FileSegmentType.INDEX, MessageStoreUtil.toFilePath(mq), 100L, storeExecutor);
        Assert.assertEquals(Long.MAX_VALUE, fileSegment.getMaxSize());
        fileSegment.destroyFile();
    }

    @Test
    public void unexpectedCaseTest() {
        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        FileSegmentFactory factory = new FileSegmentFactory(metadataStore, storeConfig, new MessageStoreExecutor());
        FileSegment fileSegment = factory.createCommitLogFileSegment(MessageStoreUtil.toFilePath(mq), baseOffset);

        fileSegment.initPosition(fileSegment.getSize());
        Assert.assertFalse(fileSegment.needCommit());
        Assert.assertTrue(fileSegment.commitAsync().join());

        fileSegment.append(ByteBuffer.allocate(0), 0L);
        Assert.assertTrue(fileSegment.commitAsync().join());

        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(0L);
        byteBuffer.flip();
        fileSegment.append(byteBuffer, 0L);

        byteBuffer.getLong();
        Assert.assertTrue(fileSegment.commitAsync().join());
        fileSegment.destroyFile();
    }

    @Test
    public void commitLogTest() throws InterruptedException {
        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        FileSegmentFactory factory = new FileSegmentFactory(metadataStore, storeConfig, new MessageStoreExecutor());
        FileSegment fileSegment = factory.createCommitLogFileSegment(MessageStoreUtil.toFilePath(mq), baseOffset);
        long lastSize = fileSegment.getSize();
        fileSegment.initPosition(fileSegment.getSize());
        Assert.assertFalse(fileSegment.needCommit());
        Assert.assertTrue(fileSegment.commitAsync().join());

        fileSegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);
        fileSegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);
        Assert.assertTrue(fileSegment.needCommit());

        fileSegment.commitLock.acquire();
        Assert.assertFalse(fileSegment.commitAsync().join());
        fileSegment.commitLock.release();

        long storeTimestamp = System.currentTimeMillis();
        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, storeTimestamp);
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 100L);
        fileSegment.append(buffer, storeTimestamp);

        Assert.assertTrue(fileSegment.needCommit());
        Assert.assertEquals(baseOffset, fileSegment.getBaseOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, fileSegment.getAppendOffset());
        Assert.assertEquals(0L, fileSegment.getMinTimestamp());
        Assert.assertEquals(storeTimestamp, fileSegment.getMaxTimestamp());

        List<ByteBuffer> buffers = fileSegment.borrowBuffer();
        Assert.assertEquals(3, buffers.size());
        fileSegment.bufferList.addAll(buffers);

        fileSegment.commitAsync().join();
        Assert.assertFalse(fileSegment.needCommit());
        Assert.assertEquals(fileSegment.getCommitOffset(), fileSegment.getAppendOffset());

        // offset will change when type is commitLog
        ByteBuffer msg1 = fileSegment.read(lastSize, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize, MessageFormatUtil.getCommitLogOffset(msg1));

        ByteBuffer msg2 = fileSegment.read(lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtil.getCommitLogOffset(msg2));

        ByteBuffer msg3 = fileSegment.read(lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtil.getCommitLogOffset(msg3));

        // buffer full
        fileSegment.bufferList.addAll(buffers);
        storeConfig.setTieredStoreMaxGroupCommitCount(3);
        Assert.assertEquals(AppendResult.BUFFER_FULL,
            fileSegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L));

        // file full
        fileSegment.initPosition(storeConfig.getTieredStoreCommitLogMaxSize() - MessageFormatUtilTest.MSG_LEN + 1);
        Assert.assertEquals(AppendResult.FILE_FULL,
            fileSegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L));

        // file close
        fileSegment.close();
        Assert.assertEquals(AppendResult.FILE_CLOSED,
            fileSegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L));
        Assert.assertFalse(fileSegment.commitAsync().join());

        fileSegment.destroyFile();
    }

    @Test
    public void consumeQueueTest() throws ClassNotFoundException, NoSuchMethodException {
        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        FileSegmentFactory factory = new FileSegmentFactory(metadataStore, storeConfig, new MessageStoreExecutor());
        FileSegment fileSegment = factory.createConsumeQueueFileSegment(MessageStoreUtil.toFilePath(mq), baseOffset);

        long storeTimestamp = System.currentTimeMillis();
        int messageSize = MessageFormatUtilTest.MSG_LEN;
        int unitSize = MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
        long initPosition = 5 * unitSize;

        fileSegment.initPosition(initPosition);
        fileSegment.append(MessageFormatUtilTest.buildMockedConsumeQueueBuffer().putLong(0, baseOffset), 0);
        fileSegment.append(MessageFormatUtilTest.buildMockedConsumeQueueBuffer().putLong(0, baseOffset + messageSize), 0);
        fileSegment.append(MessageFormatUtilTest.buildMockedConsumeQueueBuffer().putLong(0, baseOffset + messageSize * 2), storeTimestamp);

        Assert.assertEquals(initPosition + unitSize * 3, fileSegment.getAppendPosition());
        Assert.assertEquals(0, fileSegment.getMinTimestamp());
        Assert.assertEquals(storeTimestamp, fileSegment.getMaxTimestamp());

        fileSegment.commitAsync().join();
        Assert.assertEquals(fileSegment.getAppendOffset(), fileSegment.getCommitOffset());

        ByteBuffer cqItem1 = fileSegment.read(initPosition, unitSize);
        Assert.assertEquals(baseOffset, cqItem1.getLong());

        ByteBuffer cqItem2 = fileSegment.read(initPosition + unitSize, unitSize);
        Assert.assertEquals(baseOffset + messageSize, cqItem2.getLong());

        ByteBuffer cqItem3 = fileSegment.read(initPosition + unitSize * 2, unitSize);
        Assert.assertEquals(baseOffset + messageSize * 2, cqItem3.getLong());
    }

    @Test
    public void fileSegmentReadTest() throws ClassNotFoundException, NoSuchMethodException {
        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        FileSegmentFactory factory = new FileSegmentFactory(metadataStore, storeConfig, new MessageStoreExecutor());
        FileSegment fileSegment = factory.createConsumeQueueFileSegment(MessageStoreUtil.toFilePath(mq), baseOffset);

        long storeTimestamp = System.currentTimeMillis();
        int messageSize = MessageFormatUtilTest.MSG_LEN;
        int unitSize = MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
        long initPosition = 5 * unitSize;

        fileSegment.initPosition(initPosition);
        fileSegment.append(MessageFormatUtilTest.buildMockedConsumeQueueBuffer().putLong(0, baseOffset), 0);
        fileSegment.append(MessageFormatUtilTest.buildMockedConsumeQueueBuffer().putLong(0, baseOffset + messageSize), 0);
        fileSegment.append(MessageFormatUtilTest.buildMockedConsumeQueueBuffer().putLong(0, baseOffset + messageSize * 2), storeTimestamp);
        fileSegment.commitAsync().join();

        CompletionException exception = Assert.assertThrows(
            CompletionException.class, () -> fileSegment.read(-1, -1));
        Assert.assertTrue(exception.getCause() instanceof TieredStoreException);
        Assert.assertEquals(TieredStoreErrorCode.ILLEGAL_PARAM, ((TieredStoreException) exception.getCause()).getErrorCode());

        exception = Assert.assertThrows(
            CompletionException.class, () -> fileSegment.read(100, 0));
        Assert.assertTrue(exception.getCause() instanceof TieredStoreException);
        Assert.assertEquals(TieredStoreErrorCode.ILLEGAL_PARAM, ((TieredStoreException) exception.getCause()).getErrorCode());

        // at most three messages
        Assert.assertEquals(unitSize * 3,
            fileSegment.read(100, messageSize * 3).remaining());
        Assert.assertEquals(unitSize * 3,
            fileSegment.read(100, messageSize * 5).remaining());
    }

    @Test
    public void commitFailedThenSuccessTest() {
        MemoryFileSegment segment = new MemoryFileSegment(
            storeConfig, FileSegmentType.COMMIT_LOG, MessageStoreUtil.toFilePath(mq), baseOffset, storeExecutor);

        long lastSize = segment.getSize();
        segment.setCheckSize(false);
        segment.initPosition(lastSize);
        segment.setSize((int) lastSize);

        int messageSize = MessageFormatUtilTest.MSG_LEN;
        ByteBuffer buffer1 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize);
        ByteBuffer buffer2 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize + messageSize);
        Assert.assertEquals(AppendResult.SUCCESS, segment.append(buffer1, 0));
        Assert.assertEquals(AppendResult.SUCCESS, segment.append(buffer2, 0));

        // Mock new message arrive
        long timestamp = System.currentTimeMillis();
        segment.blocker = new CompletableFuture<>();
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
            ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            buffer.putLong(MessageFormatUtil.PHYSICAL_OFFSET_POSITION, messageSize * 2);
            buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp);
            segment.append(buffer, 0);
            segment.blocker.complete(false);
        }).start();

        // Commit failed
        segment.commitAsync().join();
        segment.blocker.join();
        segment.blocker = null;

        // Copy data and assume commit success
        segment.getMemStore().put(buffer1);
        segment.getMemStore().put(buffer2);
        segment.setSize((int) (lastSize + messageSize * 2));

        segment.commitAsync().join();
        Assert.assertEquals(lastSize + messageSize * 3, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize + messageSize * 3, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + messageSize * 3, segment.getAppendOffset());

        ByteBuffer msg1 = segment.read(lastSize, messageSize);
        Assert.assertEquals(baseOffset + lastSize, MessageFormatUtil.getCommitLogOffset(msg1));

        ByteBuffer msg2 = segment.read(lastSize + messageSize, messageSize);
        Assert.assertEquals(baseOffset + lastSize + messageSize, MessageFormatUtil.getCommitLogOffset(msg2));

        ByteBuffer msg3 = segment.read(lastSize + messageSize * 2, messageSize);
        Assert.assertEquals(baseOffset + lastSize + messageSize * 2, MessageFormatUtil.getCommitLogOffset(msg3));
    }

    @Test
    public void commitFailedMoreTimes() {
        long startTime = System.currentTimeMillis();
        MemoryFileSegment segment = new MemoryFileSegment(
            storeConfig, FileSegmentType.COMMIT_LOG, MessageStoreUtil.toFilePath(mq), baseOffset, storeExecutor);

        long lastSize = segment.getSize();
        segment.setCheckSize(false);
        segment.initPosition(lastSize);
        segment.setSize((int) lastSize);

        ByteBuffer buffer1 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize);
        ByteBuffer buffer2 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN);
        segment.append(buffer1, 0);
        segment.append(buffer2, 0);

        // Mock new message arrive
        segment.blocker = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
            ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            buffer.putLong(MessageFormatUtil.PHYSICAL_OFFSET_POSITION, MessageFormatUtilTest.MSG_LEN * 2);
            buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, startTime);
            segment.append(buffer, 0);
            segment.blocker.complete(false);
        }).start();

        for (int i = 0; i < 3; i++) {
            Assert.assertFalse(segment.commitAsync().join());
        }

        FileSegment fileSpySegment = Mockito.spy(segment);
        Mockito.when(fileSpySegment.getSize()).thenReturn(-1L);
        Assert.assertFalse(fileSpySegment.commitAsync().join());

        Assert.assertEquals(lastSize, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());

        segment.blocker.join();
        segment.blocker = null;

        segment.commitAsync().join();
        Assert.assertEquals(lastSize + MessageFormatUtilTest.MSG_LEN * 2, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 2, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());

        segment.commitAsync().join();
        Assert.assertEquals(lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());

        ByteBuffer msg1 = segment.read(lastSize, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize, MessageFormatUtil.getCommitLogOffset(msg1));

        ByteBuffer msg2 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtil.getCommitLogOffset(msg2));

        ByteBuffer msg3 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtil.getCommitLogOffset(msg3));
    }

    @Test
    public void handleCommitExceptionTest() {
        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        FileSegmentFactory factory = new FileSegmentFactory(metadataStore, storeConfig, storeExecutor);

        {
            FileSegment fileSegment = factory.createCommitLogFileSegment(MessageStoreUtil.toFilePath(mq), baseOffset);
            FileSegment fileSpySegment = Mockito.spy(fileSegment);
            fileSpySegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);
            fileSpySegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);

            Mockito.when(fileSpySegment.commit0(any(), anyLong(), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.supplyAsync(() -> {
                    throw new TieredStoreException(TieredStoreErrorCode.IO_ERROR, "Test");
                }));
            Assert.assertFalse(fileSpySegment.commitAsync().join());
            fileSegment.destroyFile();
        }

        {
            FileSegment fileSegment = factory.createCommitLogFileSegment(MessageStoreUtil.toFilePath(mq), baseOffset);
            FileSegment fileSpySegment = Mockito.spy(fileSegment);
            fileSpySegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);
            fileSpySegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);

            Mockito.when(fileSpySegment.commit0(any(), anyLong(), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.supplyAsync(() -> {
                    long size = MessageFormatUtilTest.buildMockedMessageBuffer().remaining();
                    TieredStoreException exception = new TieredStoreException(TieredStoreErrorCode.IO_ERROR, "Test");
                    exception.setPosition(size * 2L);
                    throw exception;
                }));
            Assert.assertTrue(fileSpySegment.commitAsync().join());
            fileSegment.destroyFile();
        }

        {
            FileSegment fileSegment = factory.createCommitLogFileSegment(MessageStoreUtil.toFilePath(mq), baseOffset);
            FileSegment fileSpySegment = Mockito.spy(fileSegment);
            fileSpySegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);
            fileSpySegment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0L);

            Mockito.when(fileSpySegment.commit0(any(), anyLong(), anyInt(), anyBoolean()))
                .thenReturn(CompletableFuture.supplyAsync(() -> {
                    throw new RuntimeException("Runtime Error for Test");
                }));
            Mockito.when(fileSpySegment.getSize()).thenReturn(0L);
            Assert.assertFalse(fileSpySegment.commitAsync().join());
        }
    }
}
