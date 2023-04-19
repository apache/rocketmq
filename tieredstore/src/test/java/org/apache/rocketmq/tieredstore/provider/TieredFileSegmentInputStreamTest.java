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

import org.apache.rocketmq.tieredstore.container.TieredCommitLog;
import org.apache.rocketmq.tieredstore.container.TieredConsumeQueue;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtilTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TieredFileSegmentInputStreamTest {

    @Test
    public void testCommitLogTypeInputStream() {
        List<ByteBuffer> uploadBufferList = new ArrayList<>();
        int bufferSize = 0;
        for (int i = 0; i < 10; i++) {
            ByteBuffer byteBuffer = MessageBufferUtilTest.buildMockedMessageBuffer();
            uploadBufferList.add(byteBuffer);
            bufferSize += byteBuffer.remaining();
        }
        TieredFileSegment.TieredFileSegmentInputStream inputStream = new TieredFileSegment.TieredFileSegmentInputStream(
                TieredFileSegment.FileSegmentType.COMMIT_LOG, 100, uploadBufferList, null, bufferSize);
        ByteBuffer msgBuffer = ByteBuffer.allocate(MessageBufferUtilTest.MSG_LEN);
        int index = 0;
        while (true) {
            int b = inputStream.read();
            if (b == -1) break;
            msgBuffer.put((byte) b);
            if (!msgBuffer.hasRemaining()) {
                msgBuffer.flip();
                // check message
                int realPhysicalOffset = 100 + index++ * MessageBufferUtilTest.MSG_LEN;
                MessageBufferUtilTest.verifyMockedMessageBuffer(msgBuffer, realPhysicalOffset);
                msgBuffer.rewind();
            }
        }
        Assert.assertEquals(10, index);
    }

    @Test
    public void testCommitLogTypeInputStreamWithCoda() {
        List<ByteBuffer> uploadBufferList = new ArrayList<>();
        int bufferSize = 0;
        for (int i = 0; i < 10; i++) {
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

        TieredFileSegment.TieredFileSegmentInputStream inputStream = new TieredFileSegment.TieredFileSegmentInputStream(
                TieredFileSegment.FileSegmentType.COMMIT_LOG, 100, uploadBufferList, codaBuffer, bufferSize);
        ByteBuffer msgBuffer = ByteBuffer.allocate(MessageBufferUtilTest.MSG_LEN);
        int index = 0;
        boolean checkCoda = false;
        ByteBuffer checkedCodaBuffer = ByteBuffer.allocate(codaBuffer.limit());
        while (true) {
            int b = inputStream.read();
            if (b == -1) break;
            if (checkCoda) {
                checkedCodaBuffer.put((byte) b);
                continue;
            }
            msgBuffer.put((byte) b);
            if (!msgBuffer.hasRemaining()) {
                msgBuffer.flip();
                // check message
                int realPhysicalOffset = 100 + index++ * MessageBufferUtilTest.MSG_LEN;
                MessageBufferUtilTest.verifyMockedMessageBuffer(msgBuffer, realPhysicalOffset);
                msgBuffer.rewind();
                if (index == 10) {
                    // switch check coda buffer
                    checkCoda = true;
                }
            }
        }
        Assert.assertEquals(10, index);
        checkedCodaBuffer.flip();
        Assert.assertEquals(codaBufferSize, checkedCodaBuffer.remaining());
        Assert.assertEquals(TieredCommitLog.CODA_SIZE, checkedCodaBuffer.getInt());
        Assert.assertEquals(TieredCommitLog.BLANK_MAGIC_CODE, checkedCodaBuffer.getInt());
        Assert.assertEquals(timeMillis, checkedCodaBuffer.getLong());
    }

    @Test
    public void testConsumeQueueTypeInputStream() {
        List<ByteBuffer> uploadBufferList = new ArrayList<>();
        int bufferSize = 0;
        for (int i = 0; i < 10; i++) {
            ByteBuffer byteBuffer = buildMockedConsumeQueueBuffer();
            uploadBufferList.add(byteBuffer);
            bufferSize += byteBuffer.remaining();
        }
        TieredFileSegment.TieredFileSegmentInputStream inputStream = new TieredFileSegment.TieredFileSegmentInputStream(
                TieredFileSegment.FileSegmentType.CONSUME_QUEUE, 100, uploadBufferList, null, bufferSize);
        ByteBuffer msgBuffer = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        int index = 0;
        while (true) {
            int b = inputStream.read();
            if (b == -1) break;
            msgBuffer.put((byte) b);
            if (!msgBuffer.hasRemaining()) {
                msgBuffer.flip();
                // check message
                verifyMockedConsumeQueueBuffer(msgBuffer);
                msgBuffer.rewind();
                index++;
            }
        }
        Assert.assertEquals(10, index);
    }

    @Test
    public void testIndexTypeInputStream() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(24);
        byteBuffer.putLong(1);
        byteBuffer.putLong(2);
        byteBuffer.putLong(3);
        byteBuffer.flip();
        List<ByteBuffer> uploadBufferList = Arrays.asList(byteBuffer);
        TieredFileSegment.TieredFileSegmentInputStream inputStream = new TieredFileSegment.TieredFileSegmentInputStream(
                TieredFileSegment.FileSegmentType.INDEX, 100, uploadBufferList, null, byteBuffer.limit());
        ByteBuffer msgBuffer = ByteBuffer.allocate(24);
        while (true) {
            int b = inputStream.read();
            if (b == -1) break;
            msgBuffer.put((byte) b);
        }
        msgBuffer.flip();
        Assert.assertEquals(24, msgBuffer.remaining());
        Assert.assertEquals(1, msgBuffer.getLong());
        Assert.assertEquals(2, msgBuffer.getLong());
        Assert.assertEquals(3, msgBuffer.getLong());
    }

    private ByteBuffer buildMockedConsumeQueueBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        byteBuffer.putLong(1);
        byteBuffer.putInt(2);
        byteBuffer.putLong(3);
        byteBuffer.flip();
        return byteBuffer;
    }

    private void verifyMockedConsumeQueueBuffer(ByteBuffer byteBuffer) {
        Assert.assertEquals(1, byteBuffer.getLong());
        Assert.assertEquals(2, byteBuffer.getInt());
        Assert.assertEquals(3, byteBuffer.getLong());
    }
}
