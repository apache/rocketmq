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

package org.apache.rocketmq.common.message;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.rocketmq.common.message.MessageDecoder.NAME_VALUE_SEPARATOR;
import static org.apache.rocketmq.common.message.MessageDecoder.PROPERTY_SEPARATOR;
import static org.apache.rocketmq.common.message.MessageDecoder.createMessageId;
import static org.apache.rocketmq.common.message.MessageDecoder.decodeMessageId;
import static org.assertj.core.api.Assertions.assertThat;

public class MessageDecoderTest {

    @Test
    public void testDecodeProperties() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("645100FA00002A9F000000489A3AA09E");
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        {
            byte[] msgBytes = new byte[0];
            try {
                msgBytes = MessageDecoder.encode(messageExt, false);
            } catch (Exception e) {
                e.printStackTrace();
                assertThat(Boolean.FALSE).isTrue();
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
            byteBuffer.put(msgBytes);

            Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);

            assertThat(properties).isNotNull();
            assertThat("123").isEqualTo(properties.get("a"));
            assertThat("hello").isEqualTo(properties.get("b"));
            assertThat("3.14").isEqualTo(properties.get("c"));
        }

        {
            byte[] msgBytes = new byte[0];
            try {
                msgBytes = MessageDecoder.encode(messageExt, false);
            } catch (Exception e) {
                e.printStackTrace();
                assertThat(Boolean.FALSE).isTrue();
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
            byteBuffer.put(msgBytes);

            Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);

            assertThat(properties).isNotNull();
            assertThat("123").isEqualTo(properties.get("a"));
            assertThat("hello").isEqualTo(properties.get("b"));
            assertThat("3.14").isEqualTo(properties.get("c"));
        }
    }

    @Test
    public void testDecodePropertiesOnIPv6Host() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        messageExt.setBornHostV6Flag();
        messageExt.setStoreHostAddressV6Flag();
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);

        assertThat(properties).isNotNull();
        assertThat("123").isEqualTo(properties.get("a"));
        assertThat("hello").isEqualTo(properties.get("b"));
        assertThat("3.14").isEqualTo(properties.get("c"));
    }

    @Test
    public void testEncodeAndDecode() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("645100FA00002A9F000000489A3AA09E");
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(1);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        messageExt.setBodyCRC(UtilAll.crc32(messageExt.getBody()));

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        byteBuffer.flip();
        MessageExt decodedMsg = MessageDecoder.decode(byteBuffer);

        assertThat(decodedMsg).isNotNull();
        assertThat(1).isEqualTo(decodedMsg.getQueueId());
        assertThat(123456L).isEqualTo(decodedMsg.getCommitLogOffset());
        assertThat("hello!q!".getBytes()).isEqualTo(decodedMsg.getBody());

        int msgIDLength = 4 + 4 + 8;
        ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
        String msgId = createMessageId(byteBufferMsgId, messageExt.getStoreHostBytes(), messageExt.getCommitLogOffset());
        assertThat(msgId).isEqualTo(decodedMsg.getMsgId());

        assertThat("abc").isEqualTo(decodedMsg.getTopic());
    }

    @Test
    public void testEncodeAndDecodeOnIPv6Host() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        messageExt.setBornHostV6Flag();
        messageExt.setStoreHostAddressV6Flag();
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(1);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        messageExt.setBodyCRC(UtilAll.crc32(messageExt.getBody()));

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        byteBuffer.flip();
        MessageExt decodedMsg = MessageDecoder.decode(byteBuffer);

        assertThat(decodedMsg).isNotNull();
        assertThat(1).isEqualTo(decodedMsg.getQueueId());
        assertThat(123456L).isEqualTo(decodedMsg.getCommitLogOffset());
        assertThat("hello!q!".getBytes()).isEqualTo(decodedMsg.getBody());
        // assertThat(48).isEqualTo(decodedMsg.getSysFlag());
        assertThat(MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.STOREHOSTADDRESS_V6_FLAG)).isTrue();

        int msgIDLength = 16 + 4 + 8;
        ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
        String msgId = createMessageId(byteBufferMsgId, messageExt.getStoreHostBytes(), messageExt.getCommitLogOffset());
        assertThat(msgId).isEqualTo(decodedMsg.getMsgId());

        assertThat("abc").isEqualTo(decodedMsg.getTopic());
    }

    @Test
    public void testNullValueProperty() throws Exception {
        MessageExt msg = new MessageExt();
        msg.setBody("x".getBytes());
        msg.setTopic("x");
        msg.setBornHost(new InetSocketAddress("127.0.0.1", 9000));
        msg.setStoreHost(new InetSocketAddress("127.0.0.1", 9000));
        String key = "NullValueKey";
        msg.putProperty(key, null);
        try {
            byte[] encode = MessageDecoder.encode(msg, false);
            MessageExt decode = MessageDecoder.decode(ByteBuffer.wrap(encode));
            assertThat(decode.getProperty(key)).isNull();
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    @Test
    public void testString2messageProperties() {
        StringBuilder sb = new StringBuilder();
        sb.append("k1").append(NAME_VALUE_SEPARATOR).append("v1");
        Map<String,String> m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("k1")).isEqualTo("v1");

        m = MessageDecoder.string2messageProperties("");
        assertThat(m).size().isEqualTo(0);

        m = MessageDecoder.string2messageProperties(" ");
        assertThat(m).size().isEqualTo(0);

        m = MessageDecoder.string2messageProperties("aaa");
        assertThat(m).size().isEqualTo(0);

        sb.setLength(0);
        sb.append("k1").append(NAME_VALUE_SEPARATOR);
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(0);

        sb.setLength(0);
        sb.append(NAME_VALUE_SEPARATOR).append("v1");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(0);

        sb.setLength(0);
        sb.append("k1").append(NAME_VALUE_SEPARATOR).append("v1").append(PROPERTY_SEPARATOR);
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("k1")).isEqualTo("v1");

        sb.setLength(0);
        sb.append("k1").append(NAME_VALUE_SEPARATOR).append("v1").append(PROPERTY_SEPARATOR)
                .append("k2").append(NAME_VALUE_SEPARATOR).append("v2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(2);
        assertThat(m.get("k1")).isEqualTo("v1");
        assertThat(m.get("k2")).isEqualTo("v2");

        sb.setLength(0);
        sb.append("k1").append(NAME_VALUE_SEPARATOR).append("v1").append(PROPERTY_SEPARATOR)
                .append(NAME_VALUE_SEPARATOR).append("v2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("k1")).isEqualTo("v1");

        sb.setLength(0);
        sb.append("k1").append(NAME_VALUE_SEPARATOR).append("v1").append(PROPERTY_SEPARATOR)
                .append("k2").append(NAME_VALUE_SEPARATOR);
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("k1")).isEqualTo("v1");

        sb.setLength(0);
        sb.append(NAME_VALUE_SEPARATOR).append("v1").append(PROPERTY_SEPARATOR)
                .append("k2").append(NAME_VALUE_SEPARATOR).append("v2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("k2")).isEqualTo("v2");

        sb.setLength(0);
        sb.append("k1").append(NAME_VALUE_SEPARATOR).append(PROPERTY_SEPARATOR)
                .append("k2").append(NAME_VALUE_SEPARATOR).append("v2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("k2")).isEqualTo("v2");

        sb.setLength(0);
        sb.append("1").append(NAME_VALUE_SEPARATOR).append("1").append(PROPERTY_SEPARATOR)
                .append("2").append(NAME_VALUE_SEPARATOR).append("2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(2);
        assertThat(m.get("1")).isEqualTo("1");
        assertThat(m.get("2")).isEqualTo("2");

        sb.setLength(0);
        sb.append("1").append(NAME_VALUE_SEPARATOR).append(PROPERTY_SEPARATOR)
                .append("2").append(NAME_VALUE_SEPARATOR).append("2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("2")).isEqualTo("2");

        sb.setLength(0);
        sb.append(NAME_VALUE_SEPARATOR).append("1").append(PROPERTY_SEPARATOR)
                .append("2").append(NAME_VALUE_SEPARATOR).append("2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("2")).isEqualTo("2");

        sb.setLength(0);
        sb.append("1").append(NAME_VALUE_SEPARATOR).append("1").append(PROPERTY_SEPARATOR)
                .append("2").append(NAME_VALUE_SEPARATOR);
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("1")).isEqualTo("1");

        sb.setLength(0);
        sb.append("1").append(NAME_VALUE_SEPARATOR).append("1").append(PROPERTY_SEPARATOR)
                .append(NAME_VALUE_SEPARATOR).append("2");
        m = MessageDecoder.string2messageProperties(sb.toString());
        assertThat(m).size().isEqualTo(1);
        assertThat(m.get("1")).isEqualTo("1");
    }

    @Test
    public void testMessageId() throws Exception {
        // ipv4 messageId test
        MessageExt msgExt = new MessageExt();
        msgExt.setStoreHost(new InetSocketAddress("127.0.0.1", 9103));
        msgExt.setCommitLogOffset(123456);
        verifyMessageId(msgExt);

        // ipv6 messageId test
        msgExt.setStoreHostAddressV6Flag();
        msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        verifyMessageId(msgExt);
    }

    private void verifyMessageId(MessageExt msgExt) throws UnknownHostException {
        int storehostIPLength = (msgExt.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
        int msgIDLength = storehostIPLength + 4 + 8;
        ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
        String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), msgExt.getCommitLogOffset());

        MessageId messageId = decodeMessageId(msgId);
        assertThat(messageId.getAddress()).isEqualTo(msgExt.getStoreHost());
        assertThat(messageId.getOffset()).isEqualTo(msgExt.getCommitLogOffset());
    }

    /**
     * messageProperties2Bytes must produce the exact same bytes as
     * messageProperties2String(...).getBytes(UTF_8) for any well-formed input map (i.e.
     * for any map without null keys or values, which is the contract enforced by both
     * methods after the fix). Covers ASCII, multi-byte UTF-8, and a paired surrogate.
     */
    @Test
    public void testMessageProperties2BytesMatchesString() {
        java.util.Map<String, String> props = new java.util.LinkedHashMap<>();
        props.put("KEYS", "abc");                  // ASCII
        props.put("UNIQ_KEY", "value-123");        // ASCII
        props.put("\u4E2D\u6587\u952E", "\u4E2D\u6587\u503C"); // multi-byte UTF-8 (CJK)
        props.put("emoji", "a\uD83D\uDE00b");      // paired surrogate (U+1F600)

        String s = MessageDecoder.messageProperties2String(props);
        byte[] viaBytes = MessageDecoder.messageProperties2Bytes(props);

        assertThat(viaBytes).isNotNull();
        assertThat(viaBytes).isEqualTo(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Both messageProperties2String and messageProperties2Bytes must skip entries with
     * a null key or null value, keeping the two encoding paths byte-for-byte identical.
     */
    @Test
    public void testMessageProperties2StringAndBytesSkipNullKeyAndValue() {
        java.util.Map<String, String> props = new java.util.LinkedHashMap<>();
        props.put("a", "1");
        props.put(null, "skip-null-key");
        props.put("b", null);
        props.put("c", "2");

        String s = MessageDecoder.messageProperties2String(props);
        byte[] bytes = MessageDecoder.messageProperties2Bytes(props);

        // Only "a=1" and "c=2" should survive; "null" literal must not appear.
        assertThat(s).doesNotContain("null");
        assertThat(bytes).isEqualTo(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        java.util.Map<String, String> roundTrip = MessageDecoder.string2messageProperties(s);
        assertThat(roundTrip).hasSize(2);
        assertThat(roundTrip.get("a")).isEqualTo("1");
        assertThat(roundTrip.get("c")).isEqualTo("2");
    }

    /**
     * Custom UTF-8 encoder must match JDK String.getBytes(UTF_8) for unpaired surrogates,
     * which JDK replaces with the U+FFFD replacement character (3 bytes EF BF BD).
     */
    @Test
    public void testMessageProperties2BytesUnpairedSurrogateMatchesJdk() {
        // Lone high surrogate, lone low surrogate, and a high-then-non-low sequence.
        String[] malformed = new String[] {
            "\uD83D",                              // unpaired high
            "\uDE00",                              // unpaired low
            "x\uD83Dy",                            // high followed by non-low
            "x\uDE00y",                            // low followed by non-low
            "\uD83D\uD83D"                         // two highs in a row
        };

        for (String value : malformed) {
            java.util.Map<String, String> props = new java.util.LinkedHashMap<>();
            props.put("k", value);

            byte[] expected = MessageDecoder.messageProperties2String(props)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] actual = MessageDecoder.messageProperties2Bytes(props);

            assertThat(actual).as("input = %s", java.util.Arrays.toString(value.toCharArray()))
                .isEqualTo(expected);
        }
    }

    /**
     * bytes2messageProperties must return an independent (not ThreadLocal-shared) map so
     * that consecutive decodes on the same thread don't corrupt each other. Also verifies
     * the standard HashMap contract is preserved (mutable Entry.setValue).
     */
    @Test
    public void testBytes2messagePropertiesReturnsIndependentMap() {
        java.util.Map<String, String> first = new java.util.LinkedHashMap<>();
        first.put("k1", "v1");
        first.put("k2", "v2");

        java.util.Map<String, String> second = new java.util.LinkedHashMap<>();
        second.put("k3", "v3");

        byte[] firstBytes = MessageDecoder.messageProperties2Bytes(first);
        byte[] secondBytes = MessageDecoder.messageProperties2Bytes(second);

        java.util.Map<String, String> firstDecoded =
            MessageDecoder.bytes2messageProperties(firstBytes, 0, firstBytes.length);
        java.util.Map<String, String> secondDecoded =
            MessageDecoder.bytes2messageProperties(secondBytes, 0, secondBytes.length);

        // Decoding the second message must not mutate the first decoded map.
        assertThat(firstDecoded).hasSize(2);
        assertThat(firstDecoded.get("k1")).isEqualTo("v1");
        assertThat(firstDecoded.get("k2")).isEqualTo("v2");
        assertThat(secondDecoded).hasSize(1);
        assertThat(secondDecoded.get("k3")).isEqualTo("v3");

        // Standard HashMap contract: Entry.setValue must work.
        for (Map.Entry<String, String> e : firstDecoded.entrySet()) {
            e.setValue("mutated");
        }
        assertThat(firstDecoded.get("k1")).isEqualTo("mutated");
    }

    /**
     * messageProperties2Bytes returns null for null/empty input; callers (broker encoders)
     * treat null and a 0-length byte[] identically. The Javadoc must reflect this.
     */
    @Test
    public void testMessageProperties2BytesNullAndEmpty() {
        assertThat(MessageDecoder.messageProperties2Bytes(null)).isNull();
        assertThat(MessageDecoder.messageProperties2Bytes(new java.util.HashMap<>())).isNull();

        // A map containing only null-valued entries also produces no bytes.
        java.util.Map<String, String> nullOnly = new java.util.HashMap<>();
        nullOnly.put("k", null);
        assertThat(MessageDecoder.messageProperties2Bytes(nullOnly)).isNull();
    }
}