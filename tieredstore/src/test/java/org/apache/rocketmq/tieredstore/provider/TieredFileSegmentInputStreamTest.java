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

import com.google.common.base.Supplier;
import org.apache.rocketmq.tieredstore.container.TieredCommitLog;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStream;
import org.apache.rocketmq.tieredstore.provider.inputstream.TieredFileSegmentInputStreamFactory;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtilTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TieredFileSegmentInputStreamTest {

    private final static long COMMIT_LOG_START_OFFSET = 13131313;

    private final static int MSG_LEN = MessageBufferUtilTest.MSG_LEN;

    private final static int MSG_NUM = 10;

    private final static int RESET_TIMES = 10;

    private final static Random RANDOM = new Random();

    @Test
    public void testCommitLogTypeInputStream() {
        List<ByteBuffer> uploadBufferList = new ArrayList<>();
        int bufferSize = 0;
        for (int i = 0; i < MSG_NUM; i++) {
            ByteBuffer byteBuffer = MessageBufferUtilTest.buildMockedMessageBuffer();
            uploadBufferList.add(byteBuffer);
            bufferSize += byteBuffer.remaining();
        }

        // build expected byte buffer for verifying the TieredFileSegmentInputStream
        ByteBuffer expectedByteBuffer = ByteBuffer.allocate(bufferSize);
        for (ByteBuffer byteBuffer : uploadBufferList) {
            expectedByteBuffer.put(byteBuffer);
            byteBuffer.rewind();
        }
        // set real physical offset
        for (int i = 0; i < MSG_NUM; i++) {
            long physicalOffset = COMMIT_LOG_START_OFFSET + i * MSG_LEN;
            int position = i * MSG_LEN + MessageBufferUtil.PHYSICAL_OFFSET_POSITION;
            expectedByteBuffer.putLong(position, physicalOffset);
        }

        int finalBufferSize = bufferSize;
        verifyReadAndReset(expectedByteBuffer, () -> TieredFileSegmentInputStreamFactory.build(
                TieredFileSegment.FileSegmentType.COMMIT_LOG, COMMIT_LOG_START_OFFSET, uploadBufferList, null, finalBufferSize), finalBufferSize);

    }

    @Test
    public void testCommitLogTypeInputStreamWithCoda() {
        List<ByteBuffer> uploadBufferList = new ArrayList<>();
        int bufferSize = 0;
        for (int i = 0; i < MSG_NUM; i++) {
            ByteBuffer byteBuffer = MessageBufferUtilTest.buildMockedMessageBuffer();
            uploadBufferList.add(byteBuffer);
            bufferSize += byteBuffer.remaining();
        }

        ByteBuffer codaBuffer = ByteBuffer.allocate(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.CODA_SIZE);
        codaBuffer.putInt(TieredCommitLog.BLANK_MAGIC_CODE);
        long timeMillis = System.currentTimeMillis();
        codaBuffer.putLong(timeMillis);
        codaBuffer.flip();
        int codaBufferSize = codaBuffer.remaining();
        bufferSize += codaBufferSize;

        // build expected byte buffer for verifying the TieredFileSegmentInputStream
        ByteBuffer expectedByteBuffer = ByteBuffer.allocate(bufferSize);
        for (ByteBuffer byteBuffer : uploadBufferList) {
            expectedByteBuffer.put(byteBuffer);
            byteBuffer.rewind();
        }
        expectedByteBuffer.put(codaBuffer);
        codaBuffer.rewind();
        // set real physical offset
        for (int i = 0; i < MSG_NUM; i++) {
            long physicalOffset = COMMIT_LOG_START_OFFSET + i * MSG_LEN;
            int position = i * MSG_LEN + MessageBufferUtil.PHYSICAL_OFFSET_POSITION;
            expectedByteBuffer.putLong(position, physicalOffset);
        }

        int finalBufferSize = bufferSize;
        verifyReadAndReset(expectedByteBuffer, () -> TieredFileSegmentInputStreamFactory.build(
                TieredFileSegment.FileSegmentType.COMMIT_LOG, COMMIT_LOG_START_OFFSET, uploadBufferList, codaBuffer, finalBufferSize), finalBufferSize);

    }

    @Test
    public void testConsumeQueueTypeInputStream() {
        List<ByteBuffer> uploadBufferList = new ArrayList<>();
        int bufferSize = 0;
        for (int i = 0; i < MSG_NUM; i++) {
            ByteBuffer byteBuffer = MessageBufferUtilTest.buildMockedConsumeQueueBuffer();
            uploadBufferList.add(byteBuffer);
            bufferSize += byteBuffer.remaining();
        }

        // build expected byte buffer for verifying the TieredFileSegmentInputStream
        ByteBuffer expectedByteBuffer = ByteBuffer.allocate(bufferSize);
        for (ByteBuffer byteBuffer : uploadBufferList) {
            expectedByteBuffer.put(byteBuffer);
            byteBuffer.rewind();
        }

        int finalBufferSize = bufferSize;
        verifyReadAndReset(expectedByteBuffer, () -> TieredFileSegmentInputStreamFactory.build(
                TieredFileSegment.FileSegmentType.CONSUME_QUEUE, COMMIT_LOG_START_OFFSET, uploadBufferList, null, finalBufferSize), bufferSize);

    }

    @Test
    public void testIndexTypeInputStream() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(24);
        byteBuffer.putLong(1);
        byteBuffer.putLong(2);
        byteBuffer.putLong(3);
        byteBuffer.flip();
        List<ByteBuffer> uploadBufferList = Arrays.asList(byteBuffer);

        // build expected byte buffer for verifying the TieredFileSegmentInputStream
        ByteBuffer expectedByteBuffer = byteBuffer.slice();

        verifyReadAndReset(expectedByteBuffer, () -> TieredFileSegmentInputStreamFactory.build(
                TieredFileSegment.FileSegmentType.INDEX, COMMIT_LOG_START_OFFSET, uploadBufferList, null, byteBuffer.limit()), byteBuffer.limit());
    }

    private void verifyReadAndReset(ByteBuffer expectedByteBuffer, Supplier<TieredFileSegmentInputStream> constructor, int bufferSize) {
        TieredFileSegmentInputStream inputStream = constructor.get();

        // verify
        verifyInputStream(inputStream, expectedByteBuffer);

        // verify reset with method InputStream#mark() hasn't been called
        try {
            inputStream.reset();
            Assert.fail("Should throw IOException");
        } catch (IOException e) {
            Assert.assertTrue(e instanceof IOException);
        }

        // verify reset with method InputStream#mark() has been called
        int resetPosition = RANDOM.nextInt(bufferSize);
        int expectedResetPosition = 0;
        inputStream = constructor.get();
        for (int i = 0; i < RESET_TIMES; i++) {
            // verify and mark with resetPosition
            verifyInputStream(inputStream, expectedByteBuffer, expectedResetPosition, resetPosition);

            try {
                inputStream.reset();
            } catch (IOException e) {
                Assert.fail("Should not throw IOException");
            }

            expectedResetPosition = resetPosition;
            resetPosition = RANDOM.nextInt(bufferSize - resetPosition) + resetPosition;
        }
    }

    private void verifyInputStream(InputStream inputStream, ByteBuffer expectedBuffer) {
        verifyInputStream(inputStream, expectedBuffer, 0, -1);
    }

    /**
     * verify the input stream
     * @param inputStream the input stream to be verified
     * @param expectedBuffer the expected byte buffer
     * @param expectedBufferReadPos the expected start position of the expected byte buffer
     * @param expectedMarkCalledPos the expected position when the method InputStream#mark() is called. <i>(-1 means ignored)</i>
     */
    private void verifyInputStream(InputStream inputStream, ByteBuffer expectedBuffer, int expectedBufferReadPos, int expectedMarkCalledPos) {
        try {
            expectedBuffer.position(expectedBufferReadPos);
            while (true) {
                if (expectedMarkCalledPos == expectedBuffer.position()) {
                    inputStream.mark(0);
                }
                int b = inputStream.read();
                if (b == -1) break;
                Assert.assertEquals(expectedBuffer.get(), (byte) b);
            }
            Assert.assertFalse(expectedBuffer.hasRemaining());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

}
