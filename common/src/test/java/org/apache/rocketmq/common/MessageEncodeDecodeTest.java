/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class MessageEncodeDecodeTest {

    @Test
    public void testEncodeDecodeSingle() throws Exception {
        Message message = new Message("topic", "body".getBytes());
        message.setFlag(12);
        message.putUserProperty("key", "value");
        byte[] bytes = MessageDecoder.encodeMessage(message);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        Message newMessage = MessageDecoder.decodeMessage(buffer);

        assertTrue(message.getFlag() == newMessage.getFlag());
        assertTrue(newMessage.getProperty("key").equals(newMessage.getProperty("key")));
        assertTrue(Arrays.equals(newMessage.getBody(), message.getBody()));
    }

    @Test
    public void testEncodeDecodeList() throws Exception {
        List<Message> messages = new ArrayList<>(128);
        for (int i = 0; i < 100; i++) {
            Message message = new Message("topic", ("body" + i).getBytes());
            message.setFlag(i);
            message.putUserProperty("key", "value" + i);
            messages.add(message);
        }
        byte[] bytes = MessageDecoder.encodeMessages(messages);

        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        List<Message> newMsgs = MessageDecoder.decodeMessages(buffer);

        assertTrue(newMsgs.size() == messages.size());

        for (int i = 0; i < newMsgs.size(); i++) {
            Message message = messages.get(i);
            Message newMessage = newMsgs.get(i);
            assertTrue(message.getFlag() == newMessage.getFlag());
            assertTrue(newMessage.getProperty("key").equals(newMessage.getProperty("key")));
            assertTrue(Arrays.equals(newMessage.getBody(), message.getBody()));

        }
    }

    @Test
    public void testMessageEncodingWireFormatGoldenBytes() {
        Message message = new Message();
        message.setBody(new byte[] {0x01, 0x02});
        message.setFlag(0x01020304);

        byte[] encodedProperties = new byte[0];
        int storeSize = 4 + 4 + 4 + 4 + 4 + message.getBody().length + 2 + encodedProperties.length;
        ByteBuffer expected = ByteBuffer.allocate(storeSize);
        expected.putInt(storeSize);
        expected.putInt(0);
        expected.putInt(0);
        expected.putInt(message.getFlag());
        expected.putInt(message.getBody().length);
        expected.put(message.getBody());
        expected.putShort((short) encodedProperties.length);
        expected.put(encodedProperties);

        assertArrayEquals(expected.array(), MessageDecoder.encodeMessage(message));
        assertArrayEquals(expected.array(), MessageDecoder.encodeMessages(Arrays.asList(message)));
    }

    @Test
    public void testBatchEncodingMatchesConcatenatedSingleMessages() {
        Message emptyMessage = new Message();
        emptyMessage.setBody(new byte[0]);
        emptyMessage.setFlag(-1);

        Message unicodeMessage = new Message("topic", "payload".getBytes(MessageDecoder.CHARSET_UTF8));
        unicodeMessage.setFlag(7);
        unicodeMessage.putUserProperty("region", "\u534e\u4e1c");

        byte[] largeBody = new byte[4096];
        Arrays.fill(largeBody, (byte) 0x5A);
        Message largeMessage = new Message("topic", largeBody);
        largeMessage.setFlag(Integer.MAX_VALUE);
        largeMessage.putUserProperty("key", "value");
        largeMessage.putUserProperty("trace", "enabled");

        List<Message> messages = Arrays.asList(emptyMessage, unicodeMessage, largeMessage);
        List<byte[]> individuallyEncoded = new ArrayList<>(messages.size());
        int expectedLength = 0;
        for (Message message : messages) {
            byte[] encoded = MessageDecoder.encodeMessage(message);
            individuallyEncoded.add(encoded);
            expectedLength += encoded.length;
        }

        ByteBuffer expected = ByteBuffer.allocate(expectedLength);
        for (byte[] encoded : individuallyEncoded) {
            expected.put(encoded);
        }

        assertArrayEquals(expected.array(), MessageDecoder.encodeMessages(messages));
        assertArrayEquals(new byte[0], MessageDecoder.encodeMessages(new ArrayList<Message>()));
    }
}
