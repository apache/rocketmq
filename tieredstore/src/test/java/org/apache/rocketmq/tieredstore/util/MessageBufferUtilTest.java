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
package org.apache.rocketmq.tieredstore.util;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
import org.apache.rocketmq.tieredstore.file.TieredCommitLog;
import org.apache.rocketmq.tieredstore.file.TieredConsumeQueue;
import org.junit.Assert;
import org.junit.Test;

public class MessageBufferUtilTest {
    public static final int MSG_LEN = 4 //TOTALSIZE
        + 4 //MAGICCODE
        + 4 //BODYCRC
        + 4 //QUEUEID
        + 4 //FLAG
        + 8 //QUEUEOFFSET
        + 8 //PHYSICALOFFSET
        + 4 //SYSFLAG
        + 8 //BORNTIMESTAMP
        + 8 //BORNHOST
        + 8 //STORETIMESTAMP
        + 8 //STOREHOSTADDRESS
        + 4 //RECONSUMETIMES
        + 8 //Prepared Transaction Offset
        + 4 + 0 //BODY
        + 2 + 0 //TOPIC
        + 2 + 31 //properties
        + 0;

    public static ByteBuffer buildMockedMessageBuffer() {
        // Initialization of storage space
        ByteBuffer buffer = ByteBuffer.allocate(MSG_LEN);
        // 1 TOTALSIZE
        buffer.putInt(MSG_LEN);
        // 2 MAGICCODE
        buffer.putInt(MessageDecoder.MESSAGE_MAGIC_CODE_V2);
        // 3 BODYCRC
        buffer.putInt(3);
        // 4 QUEUEID
        buffer.putInt(4);
        // 5 FLAG
        buffer.putInt(5);
        // 6 QUEUEOFFSET
        buffer.putLong(6);
        // 7 PHYSICALOFFSET
        buffer.putLong(7);
        // 8 SYSFLAG
        buffer.putInt(8);
        // 9 BORNTIMESTAMP
        buffer.putLong(9);
        // 10 BORNHOST
        buffer.putLong(10);
        // 11 STORETIMESTAMP
        buffer.putLong(11);
        // 12 STOREHOSTADDRESS
        buffer.putLong(10);
        // 13 RECONSUMETIMES
        buffer.putInt(13);
        // 14 Prepared Transaction Offset
        buffer.putLong(14);
        // 15 BODY
        buffer.putInt(0);
        // 16 TOPIC
        buffer.putShort((short) 0);
        // 17 PROPERTIES
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "uk");
        map.put("userkey", "uservalue0");
        String properties = MessageDecoder.messageProperties2String(map);
        byte[] propertiesBytes = properties.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) propertiesBytes.length);
        buffer.put(propertiesBytes);
        buffer.flip();

        Assert.assertEquals(MSG_LEN, buffer.remaining());
        return buffer;
    }

    public static ByteBuffer buildMockedConsumeQueueBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        // 1 COMMIT_LOG_OFFSET
        byteBuffer.putLong(1);
        // 2 MESSAGE_SIZE
        byteBuffer.putInt(2);
        // 3 TAG_HASH_CODE
        byteBuffer.putLong(3);
        byteBuffer.flip();
        return byteBuffer;
    }

    public static void verifyMockedMessageBuffer(ByteBuffer buffer, int phyOffset) {
        Assert.assertEquals(MSG_LEN, buffer.remaining());
        Assert.assertEquals(MSG_LEN, buffer.getInt());
        Assert.assertEquals(MessageDecoder.MESSAGE_MAGIC_CODE_V2, buffer.getInt());
        Assert.assertEquals(3, buffer.getInt());
        Assert.assertEquals(4, buffer.getInt());
        Assert.assertEquals(5, buffer.getInt());
        Assert.assertEquals(6, buffer.getLong());
        Assert.assertEquals(phyOffset, buffer.getLong());
        Assert.assertEquals(8, buffer.getInt());
        Assert.assertEquals(9, buffer.getLong());
        Assert.assertEquals(10, buffer.getLong());
        Assert.assertEquals(11, buffer.getLong());
        Assert.assertEquals(10, buffer.getLong());
        Assert.assertEquals(13, buffer.getInt());
        Assert.assertEquals(14, buffer.getLong());
        Assert.assertEquals(0, buffer.getInt());
        Assert.assertEquals(0, buffer.getShort());
        buffer.rewind();
        Map<String, String> properties = MessageBufferUtil.getProperties(buffer);
        Assert.assertEquals("uk", properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        Assert.assertEquals("uservalue0", properties.get("userkey"));
    }

    @Test
    public void testGetTotalSize() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        int totalSize = MessageBufferUtil.getTotalSize(buffer);
        Assert.assertEquals(MSG_LEN, totalSize);
    }

    @Test
    public void testGetMagicCode() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        int magicCode = MessageBufferUtil.getMagicCode(buffer);
        Assert.assertEquals(MessageDecoder.MESSAGE_MAGIC_CODE_V2, magicCode);
    }

    @Test
    public void testSplitMessages() {
        ByteBuffer msgBuffer1 = buildMockedMessageBuffer();
        msgBuffer1.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 10);

        ByteBuffer msgBuffer2 = ByteBuffer.allocate(TieredCommitLog.CODA_SIZE);
        msgBuffer2.putInt(TieredCommitLog.CODA_SIZE);
        msgBuffer2.putInt(TieredCommitLog.BLANK_MAGIC_CODE);
        msgBuffer2.putLong(System.currentTimeMillis());
        msgBuffer2.flip();

        ByteBuffer msgBuffer3 = buildMockedMessageBuffer();
        msgBuffer3.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 11);

        ByteBuffer msgBuffer = ByteBuffer.allocate(
            msgBuffer1.remaining() + msgBuffer2.remaining() + msgBuffer3.remaining());
        msgBuffer.put(msgBuffer1);
        msgBuffer.put(msgBuffer2);
        msgBuffer.put(msgBuffer3);
        msgBuffer.flip();

        ByteBuffer cqBuffer1 = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqBuffer1.putLong(1000);
        cqBuffer1.putInt(MSG_LEN);
        cqBuffer1.putLong(0);
        cqBuffer1.flip();

        ByteBuffer cqBuffer2 = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqBuffer2.putLong(1000 + TieredCommitLog.CODA_SIZE + MSG_LEN);
        cqBuffer2.putInt(MSG_LEN);
        cqBuffer2.putLong(0);
        cqBuffer2.flip();

        ByteBuffer cqBuffer3 = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqBuffer3.putLong(1000 + MSG_LEN);
        cqBuffer3.putInt(MSG_LEN);
        cqBuffer3.putLong(0);
        cqBuffer3.flip();

        ByteBuffer cqBuffer4 = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqBuffer4.putLong(1000 + TieredCommitLog.CODA_SIZE + MSG_LEN);
        cqBuffer4.putInt(MSG_LEN - 10);
        cqBuffer4.putLong(0);
        cqBuffer4.flip();

        ByteBuffer cqBuffer5 = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqBuffer5.putLong(1000 + TieredCommitLog.CODA_SIZE + MSG_LEN);
        cqBuffer5.putInt(MSG_LEN * 10);
        cqBuffer5.putLong(0);
        cqBuffer5.flip();

        ByteBuffer cqBuffer = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE * 2);
        cqBuffer.put(cqBuffer1);
        cqBuffer.put(cqBuffer2);
        cqBuffer.flip();
        cqBuffer1.rewind();
        cqBuffer2.rewind();
        List<SelectBufferResult> msgList = MessageBufferUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(2, msgList.size());
        Assert.assertEquals(0, msgList.get(0).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(0).getSize());
        Assert.assertEquals(MSG_LEN + TieredCommitLog.CODA_SIZE, msgList.get(1).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(1).getSize());

        cqBuffer = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE * 2);
        cqBuffer.put(cqBuffer1);
        cqBuffer.put(cqBuffer4);
        cqBuffer.flip();
        cqBuffer1.rewind();
        cqBuffer4.rewind();
        msgList = MessageBufferUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(1, msgList.size());
        Assert.assertEquals(0, msgList.get(0).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(0).getSize());

        cqBuffer = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE * 3);
        cqBuffer.put(cqBuffer1);
        cqBuffer.put(cqBuffer3);
        cqBuffer.flip();
        msgList = MessageBufferUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(2, msgList.size());
        Assert.assertEquals(0, msgList.get(0).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(0).getSize());
        Assert.assertEquals(MSG_LEN + TieredCommitLog.CODA_SIZE, msgList.get(1).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(1).getSize());

        cqBuffer = ByteBuffer.allocate(TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqBuffer.put(cqBuffer5);
        cqBuffer.flip();
        msgList = MessageBufferUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(0, msgList.size());
    }

    @Test
    public void testGetQueueOffset() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        long queueOffset = MessageBufferUtil.getQueueOffset(buffer);
        Assert.assertEquals(6, queueOffset);
    }

    @Test
    public void testGetStoreTimeStamp() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        long storeTimeStamp = MessageBufferUtil.getStoreTimeStamp(buffer);
        Assert.assertEquals(11, storeTimeStamp);
    }

    @Test
    public void testGetOffsetId() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        InetSocketAddress inetSocketAddress = new InetSocketAddress("255.255.255.255", 65535);
        ByteBuffer addr = ByteBuffer.allocate(Long.BYTES);
        addr.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
        addr.putInt(inetSocketAddress.getPort());
        addr.flip();
        for (int i = 0; i < addr.remaining(); i++) {
            buffer.put(MessageBufferUtil.STORE_HOST_POSITION + i, addr.get(i));
        }
        String excepted = MessageDecoder.createMessageId(ByteBuffer.allocate(TieredStoreUtil.MSG_ID_LENGTH), addr, 7);
        String offsetId = MessageBufferUtil.getOffsetId(buffer);
        Assert.assertEquals(excepted, offsetId);
    }

    @Test
    public void testGetProperties() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        Map<String, String> properties = MessageBufferUtil.getProperties(buffer);
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.containsKey(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        Assert.assertEquals("uk", properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        Assert.assertTrue(properties.containsKey("userkey"));
        Assert.assertEquals("uservalue0", properties.get("userkey"));
    }
}
