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
import org.junit.Assert;
import org.junit.Test;

import static org.apache.rocketmq.tieredstore.util.MessageFormatUtil.COMMIT_LOG_CODA_SIZE;

public class MessageFormatUtilTest {

    public static final int MSG_LEN = 123;

    public static ByteBuffer buildMockedMessageBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(MSG_LEN);
        buffer.putInt(MSG_LEN);
        buffer.putInt(MessageDecoder.MESSAGE_MAGIC_CODE_V2);
        buffer.putInt(3);
        buffer.putInt(4);
        buffer.putInt(5);
        buffer.putLong(6);
        buffer.putLong(7);
        buffer.putInt(8);
        buffer.putLong(9);
        buffer.putLong(10);
        buffer.putLong(11);
        buffer.putLong(10);
        buffer.putInt(13);
        buffer.putLong(14);
        buffer.putInt(0);
        buffer.putShort((short) 0);

        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "uk");
        map.put("UserKey", "UserValue0");
        String properties = MessageDecoder.messageProperties2String(map);
        byte[] propertiesBytes = properties.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) propertiesBytes.length);
        buffer.put(propertiesBytes);
        buffer.flip();

        Assert.assertEquals(MSG_LEN, buffer.remaining());
        return buffer;
    }

    @Test
    public void verifyMockedMessageBuffer() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        Assert.assertEquals(MSG_LEN, buffer.remaining());
        Assert.assertEquals(MSG_LEN, buffer.getInt());
        Assert.assertEquals(MessageDecoder.MESSAGE_MAGIC_CODE_V2, buffer.getInt());
        Assert.assertEquals(3, buffer.getInt());
        Assert.assertEquals(4, buffer.getInt());
        Assert.assertEquals(5, buffer.getInt());
        Assert.assertEquals(6, buffer.getLong());
        Assert.assertEquals(7, buffer.getLong());
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
        Map<String, String> properties = MessageFormatUtil.getProperties(buffer);
        Assert.assertEquals("uk", properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        Assert.assertEquals("UserValue0", properties.get("UserKey"));
    }

    public static ByteBuffer buildMockedConsumeQueueBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        buffer.putLong(1L);
        buffer.putInt(2);
        buffer.putLong(3L);
        buffer.flip();
        return buffer;
    }

    @Test
    public void verifyMockedConsumeQueueBuffer() {
        ByteBuffer buffer = buildMockedConsumeQueueBuffer();
        Assert.assertEquals(1L, MessageFormatUtil.getCommitLogOffsetFromItem(buffer));
        Assert.assertEquals(2, MessageFormatUtil.getSizeFromItem(buffer));
        Assert.assertEquals(3L, MessageFormatUtil.getTagCodeFromItem(buffer));
    }

    @Test
    public void messageFormatBasicTest() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        Assert.assertEquals(MSG_LEN, MessageFormatUtil.getTotalSize(buffer));
        Assert.assertEquals(MessageDecoder.MESSAGE_MAGIC_CODE_V2, MessageFormatUtil.getMagicCode(buffer));
        Assert.assertEquals(6L, MessageFormatUtil.getQueueOffset(buffer));
        Assert.assertEquals(7L, MessageFormatUtil.getCommitLogOffset(buffer));
        Assert.assertEquals(11L, MessageFormatUtil.getStoreTimeStamp(buffer));
    }

    @Test
    public void getOffsetIdTest() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        InetSocketAddress inetSocketAddress = new InetSocketAddress("127.0.0.1", 65535);
        ByteBuffer address = ByteBuffer.allocate(Long.BYTES);
        address.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
        address.putInt(inetSocketAddress.getPort());
        address.flip();
        for (int i = 0; i < address.remaining(); i++) {
            buffer.put(MessageFormatUtil.STORE_HOST_POSITION + i, address.get(i));
        }
        String excepted = MessageDecoder.createMessageId(
            ByteBuffer.allocate(MessageFormatUtil.MSG_ID_LENGTH), address, 7);
        String offsetId = MessageFormatUtil.getOffsetId(buffer);
        Assert.assertEquals(excepted, offsetId);
    }

    @Test
    public void getPropertiesTest() {
        ByteBuffer buffer = buildMockedMessageBuffer();
        Map<String, String> properties = MessageFormatUtil.getProperties(buffer);
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.containsKey(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        Assert.assertEquals("uk", properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        Assert.assertTrue(properties.containsKey("UserKey"));
        Assert.assertEquals("UserValue0", properties.get("UserKey"));
    }

    @Test
    public void testSplitMessages() {
        ByteBuffer msgBuffer1 = buildMockedMessageBuffer();
        msgBuffer1.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 10);

        ByteBuffer msgBuffer2 = ByteBuffer.allocate(COMMIT_LOG_CODA_SIZE);
        msgBuffer2.putInt(MessageFormatUtil.COMMIT_LOG_CODA_SIZE);
        msgBuffer2.putInt(MessageFormatUtil.BLANK_MAGIC_CODE);
        msgBuffer2.putLong(System.currentTimeMillis());
        msgBuffer2.flip();

        ByteBuffer msgBuffer3 = buildMockedMessageBuffer();
        msgBuffer3.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 11);

        ByteBuffer msgBuffer = ByteBuffer.allocate(
            msgBuffer1.remaining() + msgBuffer2.remaining() + msgBuffer3.remaining());
        msgBuffer.put(msgBuffer1);
        msgBuffer.put(msgBuffer2);
        msgBuffer.put(msgBuffer3);
        msgBuffer.flip();

        ByteBuffer cqBuffer1 = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        cqBuffer1.putLong(1000);
        cqBuffer1.putInt(MSG_LEN);
        cqBuffer1.putLong(0);
        cqBuffer1.flip();

        ByteBuffer cqBuffer2 = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        cqBuffer2.putLong(1000 + MessageFormatUtil.COMMIT_LOG_CODA_SIZE + MSG_LEN);
        cqBuffer2.putInt(MSG_LEN);
        cqBuffer2.putLong(0);
        cqBuffer2.flip();

        ByteBuffer cqBuffer3 = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        cqBuffer3.putLong(1000 + MSG_LEN);
        cqBuffer3.putInt(MSG_LEN);
        cqBuffer3.putLong(0);
        cqBuffer3.flip();

        ByteBuffer cqBuffer4 = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        cqBuffer4.putLong(1000 + MessageFormatUtil.COMMIT_LOG_CODA_SIZE + MSG_LEN);
        cqBuffer4.putInt(MSG_LEN - 10);
        cqBuffer4.putLong(0);
        cqBuffer4.flip();

        ByteBuffer cqBuffer5 = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        cqBuffer5.putLong(1000 + MessageFormatUtil.COMMIT_LOG_CODA_SIZE + MSG_LEN);
        cqBuffer5.putInt(MSG_LEN * 10);
        cqBuffer5.putLong(0);
        cqBuffer5.flip();

        // Message buffer size is 0 or consume queue buffer size is 0
        Assert.assertEquals(0,
            MessageFormatUtil.splitMessageBuffer(null, ByteBuffer.allocate(0)).size());
        Assert.assertEquals(0,
            MessageFormatUtil.splitMessageBuffer(cqBuffer1, null).size());
        Assert.assertEquals(0,
            MessageFormatUtil.splitMessageBuffer(cqBuffer1, ByteBuffer.allocate(0)).size());
        Assert.assertEquals(0,
            MessageFormatUtil.splitMessageBuffer(ByteBuffer.allocate(0), msgBuffer).size());
        Assert.assertEquals(0,
            MessageFormatUtil.splitMessageBuffer(ByteBuffer.allocate(10), msgBuffer).size());

        ByteBuffer cqBuffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE * 2);
        cqBuffer.put(cqBuffer1);
        cqBuffer.put(cqBuffer2);
        cqBuffer.flip();
        cqBuffer1.rewind();
        cqBuffer2.rewind();
        List<SelectBufferResult> msgList = MessageFormatUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(2, msgList.size());
        Assert.assertEquals(0, msgList.get(0).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(0).getSize());
        Assert.assertEquals(MSG_LEN + MessageFormatUtil.COMMIT_LOG_CODA_SIZE, msgList.get(1).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(1).getSize());

        cqBuffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE * 2);
        cqBuffer.put(cqBuffer1);
        cqBuffer.put(cqBuffer4);
        cqBuffer.flip();
        cqBuffer1.rewind();
        cqBuffer4.rewind();
        msgList = MessageFormatUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(1, msgList.size());
        Assert.assertEquals(0, msgList.get(0).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(0).getSize());

        cqBuffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE * 3);
        cqBuffer.put(cqBuffer1);
        cqBuffer.put(cqBuffer3);
        cqBuffer.flip();
        cqBuffer1.rewind();
        cqBuffer3.rewind();
        msgList = MessageFormatUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(2, msgList.size());
        Assert.assertEquals(0, msgList.get(0).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(0).getSize());
        Assert.assertEquals(MSG_LEN + MessageFormatUtil.COMMIT_LOG_CODA_SIZE, msgList.get(1).getStartOffset());
        Assert.assertEquals(MSG_LEN, msgList.get(1).getSize());

        cqBuffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        cqBuffer.put(cqBuffer5);
        cqBuffer.flip();
        msgList = MessageFormatUtil.splitMessageBuffer(cqBuffer, msgBuffer);
        Assert.assertEquals(0, msgList.size());

        // Wrong magic code, it will destroy the mocked message buffer
        msgBuffer.putInt(MessageFormatUtil.MAGIC_CODE_POSITION, -1);
        cqBuffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE * 2);
        cqBuffer.put(cqBuffer1);
        cqBuffer.put(cqBuffer2);
        cqBuffer.flip();
        cqBuffer1.rewind();
        cqBuffer2.rewind();
        Assert.assertEquals(1, MessageFormatUtil.splitMessageBuffer(cqBuffer, msgBuffer).size());
    }
}
