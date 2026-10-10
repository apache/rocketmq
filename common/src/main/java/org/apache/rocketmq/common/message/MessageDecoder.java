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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.compression.Compressor;
import org.apache.rocketmq.common.compression.CompressorFactory;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

public class MessageDecoder {
//    public final static int MSG_ID_LENGTH = 8 + 8;

    public final static Charset CHARSET_UTF8 = StandardCharsets.UTF_8;
    public final static int MESSAGE_MAGIC_CODE_POSITION = 4;
    public final static int MESSAGE_FLAG_POSITION = 16;
    public final static int MESSAGE_PHYSIC_OFFSET_POSITION = 28;
    public final static int MESSAGE_STORE_TIMESTAMP_POSITION = 56;

    // Set message magic code v2 if topic length > 127
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    public final static int MESSAGE_MAGIC_CODE_V2 = -626843477;

    // End of file empty MAGIC CODE cbd43194
    public final static int BLANK_MAGIC_CODE = -875286124;
    public static final char NAME_VALUE_SEPARATOR = 1;
    public static final char PROPERTY_SEPARATOR = 2;
    public static final int PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8;
    public static final int QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4;
    public static final int SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8;
//    public static final int BODY_SIZE_POSITION = 4 // 1 TOTALSIZE
//        + 4 // 2 MAGICCODE
//        + 4 // 3 BODYCRC
//        + 4 // 4 QUEUEID
//        + 4 // 5 FLAG
//        + 8 // 6 QUEUEOFFSET
//        + 8 // 7 PHYSICALOFFSET
//        + 4 // 8 SYSFLAG
//        + 8 // 9 BORNTIMESTAMP
//        + 8 // 10 BORNHOST
//        + 8 // 11 STORETIMESTAMP
//        + 8 // 12 STOREHOSTADDRESS
//        + 4 // 13 RECONSUMETIMES
//        + 8; // 14 Prepared Transaction Offset

    public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
        input.flip();
        int msgIDLength = addr.limit() == 8 ? 16 : 28;
        input.limit(msgIDLength);

        input.put(addr);
        input.putLong(offset);

        return UtilAll.bytes2string(input.array());
    }

    public static String createMessageId(SocketAddress socketAddress, long transactionIdhashCode) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        int msgIDLength = inetSocketAddress.getAddress() instanceof Inet4Address ? 16 : 28;
        ByteBuffer byteBuffer = ByteBuffer.allocate(msgIDLength);
        byteBuffer.put(inetSocketAddress.getAddress().getAddress());
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.putLong(transactionIdhashCode);
        byteBuffer.flip();
        return UtilAll.bytes2string(byteBuffer.array());
    }

    public static MessageId decodeMessageId(final String msgId) throws UnknownHostException {
        byte[] bytes = UtilAll.string2bytes(msgId);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        // address(ip+port)
        byte[] ip = new byte[msgId.length() == 32 ? 4 : 16];
        byteBuffer.get(ip);
        int port = byteBuffer.getInt();
        SocketAddress address = new InetSocketAddress(InetAddress.getByAddress(ip), port);

        // offset
        long offset = byteBuffer.getLong();

        return new MessageId(address, offset);
    }

    /**
     * Just decode properties from msg buffer.
     *
     * @param byteBuffer msg commit log buffer.
     */
    public static Map<String, String> decodeProperties(ByteBuffer byteBuffer) {
        int sysFlag = byteBuffer.getInt(SYSFLAG_POSITION);
        int magicCode = byteBuffer.getInt(MESSAGE_MAGIC_CODE_POSITION);
        MessageVersion version = MessageVersion.valueOfMagicCode(magicCode);

        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        int bodySizePosition = 4 // 1 TOTALSIZE
            + 4 // 2 MAGICCODE
            + 4 // 3 BODYCRC
            + 4 // 4 QUEUEID
            + 4 // 5 FLAG
            + 8 // 6 QUEUEOFFSET
            + 8 // 7 PHYSICALOFFSET
            + 4 // 8 SYSFLAG
            + 8 // 9 BORNTIMESTAMP
            + bornhostLength // 10 BORNHOST
            + 8 // 11 STORETIMESTAMP
            + storehostAddressLength // 12 STOREHOSTADDRESS
            + 4 // 13 RECONSUMETIMES
            + 8; // 14 Prepared Transaction Offset

        int topicLengthPosition = bodySizePosition + 4 + byteBuffer.getInt(bodySizePosition);
        byteBuffer.position(topicLengthPosition);
        int topicLengthSize = version.getTopicLengthSize();
        int topicLength = version.getTopicLength(byteBuffer);

        int propertiesPosition = topicLengthPosition + topicLengthSize + topicLength;
        short propertiesLength = byteBuffer.getShort(propertiesPosition);
        byteBuffer.position(propertiesPosition + 2);

        if (propertiesLength > 0) {
            byte[] properties = new byte[propertiesLength];
            byteBuffer.get(properties);
            String propertiesString = new String(properties, CHARSET_UTF8);
            return string2messageProperties(propertiesString);
        }
        return null;
    }

    public static void createCrc32(final ByteBuffer input, int crc32) {
        input.put(MessageConst.PROPERTY_CRC32.getBytes(StandardCharsets.UTF_8));
        input.put((byte) NAME_VALUE_SEPARATOR);
        for (int i = 0; i < 10; i++) {
            byte b = '0';
            if (crc32 > 0) {
                b += (byte) (crc32 % 10);
                crc32 /= 10;
            }
            input.put(b);
        }
        input.put((byte) PROPERTY_SEPARATOR);
    }

    public static void createCrc32(final ByteBuf input, int crc32) {
        input.writeBytes(MessageConst.PROPERTY_CRC32.getBytes(StandardCharsets.UTF_8));
        input.writeByte((byte) NAME_VALUE_SEPARATOR);
        for (int i = 0; i < 10; i++) {
            byte b = '0';
            if (crc32 > 0) {
                b += (byte) (crc32 % 10);
                crc32 /= 10;
            }
            input.writeByte(b);
        }
        input.writeByte((byte) PROPERTY_SEPARATOR);
    }

    public static MessageExt decode(ByteBuffer byteBuffer) {
        return decode(byteBuffer, true, true, false);
    }

    public static MessageExt clientDecode(ByteBuffer byteBuffer, final boolean readBody) {
        return decode(byteBuffer, readBody, true, true);
    }

    public static MessageExt decode(ByteBuffer byteBuffer, final boolean readBody) {
        return decode(byteBuffer, readBody, true, false);
    }

    public static byte[] encode(MessageExt messageExt, boolean needCompress) throws Exception {
        byte[] body = messageExt.getBody();
        byte[] topics = messageExt.getTopic().getBytes(CHARSET_UTF8);
        byte topicLen = (byte) topics.length;
        String properties = messageProperties2String(messageExt.getProperties());
        byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
        short propertiesLength = (short) propertiesBytes.length;
        int sysFlag = messageExt.getSysFlag();
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        byte[] newBody = messageExt.getBody();
        if (needCompress && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
            Compressor compressor = CompressorFactory.getCompressor(MessageSysFlag.getCompressionType(sysFlag));
            newBody = compressor.compress(body, 5);
        }
        int bodyLength = newBody.length;
        int storeSize = messageExt.getStoreSize();
        ByteBuffer byteBuffer;
        if (storeSize > 0) {
            byteBuffer = ByteBuffer.allocate(storeSize);
        } else {
            storeSize = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCODE
                + 4 // 3 BODYCRC
                + 4 // 4 QUEUEID
                + 4 // 5 FLAG
                + 8 // 6 QUEUEOFFSET
                + 8 // 7 PHYSICALOFFSET
                + 4 // 8 SYSFLAG
                + 8 // 9 BORNTIMESTAMP
                + bornhostLength // 10 BORNHOST
                + 8 // 11 STORETIMESTAMP
                + storehostAddressLength // 12 STOREHOSTADDRESS
                + 4 // 13 RECONSUMETIMES
                + 8 // 14 Prepared Transaction Offset
                + 4 + bodyLength // 14 BODY
                + 1 + topicLen // 15 TOPIC
                + 2 + propertiesLength // 16 propertiesLength
                + 0;
            byteBuffer = ByteBuffer.allocate(storeSize);
        }
        // 1 TOTALSIZE
        byteBuffer.putInt(storeSize);

        // 2 MAGICCODE
        byteBuffer.putInt(MESSAGE_MAGIC_CODE);

        // 3 BODYCRC
        int bodyCRC = messageExt.getBodyCRC();
        byteBuffer.putInt(bodyCRC);

        // 4 QUEUEID
        int queueId = messageExt.getQueueId();
        byteBuffer.putInt(queueId);

        // 5 FLAG
        int flag = messageExt.getFlag();
        byteBuffer.putInt(flag);

        // 6 QUEUEOFFSET
        long queueOffset = messageExt.getQueueOffset();
        byteBuffer.putLong(queueOffset);

        // 7 PHYSICALOFFSET
        long physicOffset = messageExt.getCommitLogOffset();
        byteBuffer.putLong(physicOffset);

        // 8 SYSFLAG
        byteBuffer.putInt(sysFlag);

        // 9 BORNTIMESTAMP
        long bornTimeStamp = messageExt.getBornTimestamp();
        byteBuffer.putLong(bornTimeStamp);

        // 10 BORNHOST
        InetSocketAddress bornHost = (InetSocketAddress) messageExt.getBornHost();
        byteBuffer.put(bornHost.getAddress().getAddress());
        byteBuffer.putInt(bornHost.getPort());

        // 11 STORETIMESTAMP
        long storeTimestamp = messageExt.getStoreTimestamp();
        byteBuffer.putLong(storeTimestamp);

        // 12 STOREHOST
        InetSocketAddress serverHost = (InetSocketAddress) messageExt.getStoreHost();
        byteBuffer.put(serverHost.getAddress().getAddress());
        byteBuffer.putInt(serverHost.getPort());

        // 13 RECONSUMETIMES
        int reconsumeTimes = messageExt.getReconsumeTimes();
        byteBuffer.putInt(reconsumeTimes);

        // 14 Prepared Transaction Offset
        long preparedTransactionOffset = messageExt.getPreparedTransactionOffset();
        byteBuffer.putLong(preparedTransactionOffset);

        // 15 BODY
        byteBuffer.putInt(bodyLength);
        byteBuffer.put(newBody);

        // 16 TOPIC
        byteBuffer.put(topicLen);
        byteBuffer.put(topics);

        // 17 properties
        byteBuffer.putShort(propertiesLength);
        byteBuffer.put(propertiesBytes);

        return byteBuffer.array();
    }

    /**
     * Encode without store timestamp and store host, skip blank msg.
     *
     * @param messageExt   msg
     * @param needCompress need compress or not
     * @return byte array
     * @throws IOException when compress failed
     */
    public static byte[] encodeUniquely(MessageExt messageExt, boolean needCompress) throws IOException {
        byte[] body = messageExt.getBody();
        byte[] topics = messageExt.getTopic().getBytes(CHARSET_UTF8);
        byte topicLen = (byte) topics.length;
        String properties = messageProperties2String(messageExt.getProperties());
        byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
        short propertiesLength = (short) propertiesBytes.length;
        int sysFlag = messageExt.getSysFlag();
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        byte[] newBody = messageExt.getBody();
        if (needCompress && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
            newBody = UtilAll.compress(body, 5);
        }
        int bodyLength = newBody.length;
        int storeSize = messageExt.getStoreSize();
        ByteBuffer byteBuffer;
        if (storeSize > 0) {
            byteBuffer = ByteBuffer.allocate(storeSize - 8); // except size for store timestamp
        } else {
            storeSize = 4 +  // 1 TOTALSIZE
                4 +  // 2 MAGICCODE
                4 +  // 3 BODYCRC
                4 +  // 4 QUEUEID
                4 +  // 5 FLAG
                8 +  // 6 QUEUEOFFSET
                8 +  // 7 PHYSICALOFFSET
                4 +  // 8 SYSFLAG
                8 +  // 9 BORNTIMESTAMP
                bornhostLength + // 10 BORNHOST
                4 +  // 11 RECONSUMETIMES
                8 +  // 12 Prepared Transaction Offset
                4 + bodyLength +  // 13 BODY
                +1 + topicLen +  // 14 TOPIC
                2 + propertiesLength // 15 propertiesLength
            ;
            byteBuffer = ByteBuffer.allocate(storeSize);
        }

        // 1 TOTALSIZE
        byteBuffer.putInt(storeSize);

        // 2 MAGICCODE
        byteBuffer.putInt(MESSAGE_MAGIC_CODE);

        // 3 BODYCRC
        int bodyCRC = messageExt.getBodyCRC();
        byteBuffer.putInt(bodyCRC);

        // 4 QUEUEID
        int queueId = messageExt.getQueueId();
        byteBuffer.putInt(queueId);

        // 5 FLAG
        int flag = messageExt.getFlag();
        byteBuffer.putInt(flag);

        // 6 QUEUEOFFSET
        long queueOffset = messageExt.getQueueOffset();
        byteBuffer.putLong(queueOffset);

        // 7 PHYSICALOFFSET
        long physicOffset = messageExt.getCommitLogOffset();
        byteBuffer.putLong(physicOffset);

        // 8 SYSFLAG
        byteBuffer.putInt(sysFlag);

        // 9 BORNTIMESTAMP
        long bornTimeStamp = messageExt.getBornTimestamp();
        byteBuffer.putLong(bornTimeStamp);

        // 10 BORNHOST
        InetSocketAddress bornHost = (InetSocketAddress) messageExt.getBornHost();
        byteBuffer.put(bornHost.getAddress().getAddress());
        byteBuffer.putInt(bornHost.getPort());

        // 11 RECONSUMETIMES
        int reconsumeTimes = messageExt.getReconsumeTimes();
        byteBuffer.putInt(reconsumeTimes);

        // 12 Prepared Transaction Offset
        long preparedTransactionOffset = messageExt.getPreparedTransactionOffset();
        byteBuffer.putLong(preparedTransactionOffset);

        // 13 BODY
        byteBuffer.putInt(bodyLength);
        byteBuffer.put(newBody);

        // 14 TOPIC
        byteBuffer.put(topicLen);
        byteBuffer.put(topics);

        // 15 properties
        byteBuffer.putShort(propertiesLength);
        byteBuffer.put(propertiesBytes);

        return byteBuffer.array();
    }

    public static MessageExt decode(
        ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody) {
        return decode(byteBuffer, readBody, deCompressBody, false);
    }

    public static MessageExt decode(
        java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient) {
        return decode(byteBuffer, readBody, deCompressBody, isClient, false, false);
    }

    public static MessageExt decode(
        java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient,
        final boolean isSetPropertiesString) {
        return decode(byteBuffer, readBody, deCompressBody, isClient, isSetPropertiesString, false);
    }

    public static MessageExt decode(
        java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient,
        final boolean isSetPropertiesString, final boolean checkCRC) {
        try {

            MessageExt msgExt;
            if (isClient) {
                msgExt = new MessageClientExt();
            } else {
                msgExt = new MessageExt();
            }

            // 1 TOTALSIZE
            int storeSize = byteBuffer.getInt();
            msgExt.setStoreSize(storeSize);

            // 2 MAGICCODE
            int magicCode = byteBuffer.getInt();
            MessageVersion version = MessageVersion.valueOfMagicCode(magicCode);

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();
            msgExt.setBodyCRC(bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();
            msgExt.setQueueId(queueId);

            // 5 FLAG
            int flag = byteBuffer.getInt();
            msgExt.setFlag(flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            msgExt.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            msgExt.setCommitLogOffset(physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            msgExt.setSysFlag(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            msgExt.setBornTimestamp(bornTimeStamp);

            // 10 BORNHOST
            int bornhostIPLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 : 16;
            byte[] bornHost = new byte[bornhostIPLength];
            byteBuffer.get(bornHost, 0, bornhostIPLength);
            int port = byteBuffer.getInt();
            msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            msgExt.setStoreTimestamp(storeTimestamp);

            // 12 STOREHOST
            int storehostIPLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
            byte[] storeHost = new byte[storehostIPLength];
            byteBuffer.get(storeHost, 0, storehostIPLength);
            port = byteBuffer.getInt();
            msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();
            msgExt.setReconsumeTimes(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byte[] body = new byte[bodyLen];
                    byteBuffer.get(body);

                    if (checkCRC) {
                        //crc body
                        int crc = UtilAll.crc32(body, 0, bodyLen);
                        if (crc != bodyCRC) {
                            throw new Exception("Msg crc is error!");
                        }
                    }

                    // inflate body
                    if (deCompressBody && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
                        Compressor compressor = CompressorFactory.getCompressor(MessageSysFlag.getCompressionType(sysFlag));
                        body = compressor.decompress(body);
                        sysFlag &= ~MessageSysFlag.COMPRESSED_FLAG;
                    }

                    msgExt.setBody(body);
                    msgExt.setSysFlag(sysFlag);
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC
            int topicLen = version.getTopicLength(byteBuffer);
            byte[] topic = new byte[topicLen];
            byteBuffer.get(topic);
            msgExt.setTopic(new String(topic, CHARSET_UTF8));

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byte[] properties = new byte[propertiesLength];
                byteBuffer.get(properties);
                String propertiesString = new String(properties, CHARSET_UTF8);
                if (!isSetPropertiesString) {
                    Map<String, String> map = string2messageProperties(propertiesString);
                    msgExt.setProperties(map);
                } else {
                    Map<String, String> map = string2messageProperties(propertiesString);
                    map.put("propertiesString", propertiesString);
                    msgExt.setProperties(map);
                }
            }

            int msgIDLength = storehostIPLength + 4 + 8;
            ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
            String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), msgExt.getCommitLogOffset());
            msgExt.setMsgId(msgId);

            if (isClient) {
                ((MessageClientExt) msgExt).setOffsetMsgId(msgId);
            }

            return msgExt;
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return null;
    }

    public static List<MessageExt> decodes(ByteBuffer byteBuffer) {
        return decodes(byteBuffer, true);
    }

    public static List<MessageExt> decodesBatch(ByteBuffer byteBuffer,
        final boolean readBody,
        final boolean decompressBody,
        final boolean isClient) {
        List<MessageExt> msgExts = new ArrayList<>();
        while (byteBuffer.hasRemaining()) {
            MessageExt msgExt = decode(byteBuffer, readBody, decompressBody, isClient);
            if (null != msgExt) {
                msgExts.add(msgExt);
            } else {
                break;
            }
        }
        return msgExts;
    }

    public static List<MessageExt> decodes(ByteBuffer byteBuffer, final boolean readBody) {
        List<MessageExt> msgExts = new ArrayList<>();
        while (byteBuffer.hasRemaining()) {
            MessageExt msgExt = clientDecode(byteBuffer, readBody);
            if (null != msgExt) {
                msgExts.add(msgExt);
            } else {
                break;
            }
        }
        return msgExts;
    }

    /**
     * Per-thread reusable {@link StringBuilder} used by {@link #messageProperties2String} on
     * the hot encode path. The retained capacity is capped by {@link #REUSABLE_SB_CAP_LIMIT}
     * to avoid unbounded growth after a single oversized message.
     */
    private static final ThreadLocal<StringBuilder> REUSABLE_SB =
        ThreadLocal.withInitial(() -> new StringBuilder(256));

    /** Maximum retained capacity (in chars) of {@link #REUSABLE_SB}. */
    private static final int REUSABLE_SB_CAP_LIMIT = 64 * 1024;

    public static String messageProperties2String(Map<String, String> properties) {
        if (properties == null) {
            return "";
        }
        StringBuilder sb = REUSABLE_SB.get();
        // Trim long-lived per-thread memory if a previous message inflated the buffer.
        if (sb.capacity() > REUSABLE_SB_CAP_LIMIT) {
            sb = new StringBuilder(256);
            REUSABLE_SB.set(sb);
        }
        sb.setLength(0);
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String name = entry.getKey();
            final String value = entry.getValue();

            // Skip entries with null name or value to keep output well-formed and to
            // match {@link #messageProperties2Bytes}.
            if (name == null || value == null) {
                continue;
            }
            sb.append(name);
            sb.append(NAME_VALUE_SEPARATOR);
            sb.append(value);
            sb.append(PROPERTY_SEPARATOR);
        }
        return sb.toString();
    }

    /**
     * UTF-8 byte serialization of properties, equivalent in content to
     * {@code messageProperties2String(properties).getBytes(UTF_8)} but skipping the
     * StringBuilder + String + String.getBytes() round-trip on the broker write hot path.
     * Both this method and {@link #messageProperties2String} skip entries whose key or value
     * is {@code null}, so the encoded bytes are identical for any input map.
     * <p>Returns {@code null} for null/empty maps (encoders treat null and 0-length
     * identically); callers that need a 0-length array should check for null.
     */
    public static byte[] messageProperties2Bytes(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        int totalLen = 0;
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String name = entry.getKey();
            final String value = entry.getValue();
            if (name == null || value == null) {
                continue;
            }
            totalLen += utf8ByteLength(name);
            totalLen += utf8ByteLength(value);
            totalLen += 2;
        }
        if (totalLen == 0) {
            return null;
        }
        byte[] out = new byte[totalLen];
        int idx = 0;
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String name = entry.getKey();
            final String value = entry.getValue();
            if (name == null || value == null) {
                continue;
            }
            idx = writeUtf8(name, out, idx);
            out[idx++] = (byte) NAME_VALUE_SEPARATOR;
            idx = writeUtf8(value, out, idx);
            out[idx++] = (byte) PROPERTY_SEPARATOR;
        }
        return out;
    }

    /**
     * UTF-8 byte length of {@code s}. Matches {@link String#getBytes(java.nio.charset.Charset)
     * String.getBytes(StandardCharsets.UTF_8)} semantics: unpaired surrogate code units are
     * each replaced by a single {@code '?'} byte (the JDK's hard-coded substitution for
     * malformed UTF-8 in {@code java.lang.StringCoding}).
     */
    static int utf8ByteLength(String s) {
        int len = s.length();
        int byteLen = 0;
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                byteLen++;
            } else if (c < 0x800) {
                byteLen += 2;
            } else if (Character.isHighSurrogate(c)) {
                if (i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
                    byteLen += 4;
                    i++;
                } else {
                    // Unpaired high surrogate -> '?' (1 byte), matching JDK behavior.
                    byteLen += 1;
                }
            } else if (Character.isLowSurrogate(c)) {
                // Unpaired low surrogate -> '?' (1 byte), matching JDK behavior.
                byteLen += 1;
            } else {
                byteLen += 3;
            }
        }
        return byteLen;
    }

    /**
     * UTF-8 encode {@code s} into {@code out} starting at {@code offset}. Matches
     * {@link String#getBytes(java.nio.charset.Charset) String.getBytes(StandardCharsets.UTF_8)}
     * semantics: unpaired surrogate code units are each replaced by a single {@code '?'} byte
     * (the JDK's hard-coded substitution for malformed UTF-8 in {@code java.lang.StringCoding}).
     */
    static int writeUtf8(String s, byte[] out, int offset) {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                out[offset++] = (byte) c;
            } else if (c < 0x800) {
                out[offset++] = (byte) (0xC0 | (c >>> 6));
                out[offset++] = (byte) (0x80 | (c & 0x3F));
            } else if (Character.isHighSurrogate(c)) {
                if (i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
                    int cp = Character.toCodePoint(c, s.charAt(++i));
                    out[offset++] = (byte) (0xF0 | (cp >>> 18));
                    out[offset++] = (byte) (0x80 | ((cp >>> 12) & 0x3F));
                    out[offset++] = (byte) (0x80 | ((cp >>> 6) & 0x3F));
                    out[offset++] = (byte) (0x80 | (cp & 0x3F));
                } else {
                    // Unpaired high surrogate -> '?', matching JDK behavior.
                    out[offset++] = (byte) '?';
                }
            } else if (Character.isLowSurrogate(c)) {
                // Unpaired low surrogate -> '?', matching JDK behavior.
                out[offset++] = (byte) '?';
            } else {
                out[offset++] = (byte) (0xE0 | (c >>> 12));
                out[offset++] = (byte) (0x80 | ((c >>> 6) & 0x3F));
                out[offset++] = (byte) (0x80 | (c & 0x3F));
            }
        }
        return offset;
    }

    public static Map<String, String> string2messageProperties(final String properties) {
        return string2messageProperties(properties, 0);
    }

    /**
     * Variant of {@link #string2messageProperties(String)} that reserves capacity for
     * {@code extraEntries} additional entries the caller intends to put afterwards. Used
     * on the broker send path where MSG_REGION/TRACE_SWITCH/CLUSTER/... are appended to
     * the decoded Map; pre-sizing avoids a HashMap resize when those puts cross the load
     * factor threshold of the as-decoded capacity.
     */
    public static Map<String, String> string2messageProperties(final String properties, final int extraEntries) {
        if (properties == null || properties.isEmpty()) {
            return new HashMap<>(Math.max(4, extraEntries));
        }
        final int len = properties.length();
        int estEntries = 0;
        for (int i = 0; i < len; i++) {
            if (properties.charAt(i) == PROPERTY_SEPARATOR) {
                estEntries++;
            }
        }
        estEntries = Math.max(estEntries, 1);
        HashMap<String, String> map = new HashMap<>((estEntries + extraEntries) * 4 / 3 + 1);
        int index = 0;
        while (index < len) {
            int newIndex = properties.indexOf(PROPERTY_SEPARATOR, index);
            if (newIndex < 0) {
                newIndex = len;
            }
            if (newIndex - index >= 3) {
                int kvSepIndex = properties.indexOf(NAME_VALUE_SEPARATOR, index);
                if (kvSepIndex > index && kvSepIndex < newIndex - 1) {
                    int klen = kvSepIndex - index;
                    String k = null;
                    if (klen < MessageConst.STRING_INTERN_BY_LEN.length) {
                        String[] candidates = MessageConst.STRING_INTERN_BY_LEN[klen];
                        if (candidates != null) {
                            for (String candidate : candidates) {
                                if (properties.regionMatches(index, candidate, 0, klen)) {
                                    k = candidate;
                                    break;
                                }
                            }
                        }
                    }
                    if (k == null) {
                        k = properties.substring(index, kvSepIndex);
                    }
                    int vOffset = kvSepIndex + 1;
                    int vLen = newIndex - vOffset;
                    String v = internStringValue(properties, vOffset, vLen);
                    if (v == null) {
                        v = properties.substring(vOffset, newIndex);
                    }
                    map.put(k, v);
                }
            }
            index = newIndex + 1;
        }

        return map;
    }

    /**
     * Variant of {@link #string2messageProperties(String)} that parses directly from a UTF-8
     * byte array, skipping the intermediate {@code new String(bytes, ...)} allocation. The
     * separators {@link #NAME_VALUE_SEPARATOR} (0x01) and {@link #PROPERTY_SEPARATOR} (0x02)
     * are ASCII single bytes that never appear inside multi-byte UTF-8 sequences, so byte-level
     * scanning is safe. Canonical (intern) keys are ASCII and matched byte-by-byte.
     * <p>Always returns a fresh {@link HashMap} to keep the same downstream contract as
     * {@link #string2messageProperties} (mutable {@code Entry.setValue}, no aliasing across
     * decodes on the same thread).
     */
    public static Map<String, String> bytes2messageProperties(final byte[] bytes, final int offset,
        final int length) {
        if (bytes == null || length <= 0) {
            return new HashMap<>(4);
        }
        final int end = offset + length;
        // Estimate entries: count PROPERTY_SEPARATOR occurrences to pre-size the HashMap and
        // avoid resize churn under load.
        int estEntries = 0;
        for (int i = offset; i < end; i++) {
            if (bytes[i] == PROPERTY_SEPARATOR) {
                estEntries++;
            }
        }
        estEntries = Math.max(estEntries, 1);
        HashMap<String, String> map = new HashMap<>(estEntries * 4 / 3 + 1);
        int index = offset;
        while (index < end) {
            int sepIdx = end;
            for (int i = index; i < end; i++) {
                if (bytes[i] == PROPERTY_SEPARATOR) {
                    sepIdx = i;
                    break;
                }
            }
            if (sepIdx - index >= 3) {
                int kvSepIdx = -1;
                for (int i = index; i < sepIdx; i++) {
                    if (bytes[i] == NAME_VALUE_SEPARATOR) {
                        kvSepIdx = i;
                        break;
                    }
                }
                if (kvSepIdx > index && kvSepIdx < sepIdx - 1) {
                    int klen = kvSepIdx - index;
                    String k = null;
                    if (klen < MessageConst.STRING_INTERN_BY_LEN.length) {
                        String[] candidates = MessageConst.STRING_INTERN_BY_LEN[klen];
                        if (candidates != null) {
                            for (String candidate : candidates) {
                                if (asciiBytesEqual(bytes, index, candidate, klen)) {
                                    k = candidate;
                                    break;
                                }
                            }
                        }
                    }
                    if (k == null) {
                        k = new String(bytes, index, klen, CHARSET_UTF8);
                    }
                    int vOffset = kvSepIdx + 1;
                    int vLen = sepIdx - kvSepIdx - 1;
                    String v = internValue(bytes, vOffset, vLen);
                    if (v == null) {
                        v = new String(bytes, vOffset, vLen, CHARSET_UTF8);
                    }
                    map.put(k, v);
                }
            }
            index = sepIdx + 1;
        }
        return map;
    }

    private static final String[][] VALUE_INTERN_BY_LEN;
    static {
        String[] frequentValues = {"0", "1", "true", "false", "DefaultRegion"};
        int maxLen = 0;
        for (String s : frequentValues) {
            maxLen = Math.max(maxLen, s.length());
        }
        VALUE_INTERN_BY_LEN = new String[maxLen + 1][];
        for (String s : frequentValues) {
            int len = s.length();
            if (VALUE_INTERN_BY_LEN[len] == null) {
                VALUE_INTERN_BY_LEN[len] = new String[]{s};
            } else {
                String[] old = VALUE_INTERN_BY_LEN[len];
                String[] arr = new String[old.length + 1];
                System.arraycopy(old, 0, arr, 0, old.length);
                arr[old.length] = s;
                VALUE_INTERN_BY_LEN[len] = arr;
            }
        }
    }

    private static String internStringValue(String s, int offset, int len) {
        if (len >= VALUE_INTERN_BY_LEN.length) {
            return null;
        }
        String[] candidates = VALUE_INTERN_BY_LEN[len];
        if (candidates == null) {
            return null;
        }
        for (String candidate : candidates) {
            if (s.regionMatches(offset, candidate, 0, len)) {
                return candidate;
            }
        }
        return null;
    }

    private static String internValue(byte[] bytes, int offset, int len) {
        if (len >= VALUE_INTERN_BY_LEN.length) {
            return null;
        }
        String[] candidates = VALUE_INTERN_BY_LEN[len];
        if (candidates == null) {
            return null;
        }
        for (String candidate : candidates) {
            if (asciiBytesEqual(bytes, offset, candidate, len)) {
                return candidate;
            }
        }
        return null;
    }

    public static boolean asciiBytesMatchString(byte[] bytes, int offset, String s, int len) {
        if (s.length() != len) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (bytes[offset + i] != (byte) s.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean asciiBytesEqual(byte[] bytes, int offset, String asciiCandidate, int len) {
        for (int i = 0; i < len; i++) {
            if (bytes[offset + i] != (byte) asciiCandidate.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static byte[] encodeMessage(Message message) {
        //only need flag, body, properties
        byte[] body = message.getBody();
        int bodyLen = body.length;
        String properties = messageProperties2String(message.getProperties());
        byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
        //note properties length must not more than Short.MAX
        short propertiesLength = (short) propertiesBytes.length;
        int storeSize = 4 // 1 TOTALSIZE
            + 4 // 2 MAGICCOD
            + 4 // 3 BODYCRC
            + 4 // 4 FLAG
            + 4 + bodyLen // 4 BODY
            + 2 + propertiesLength;
        ByteBuffer byteBuffer = ByteBuffer.allocate(storeSize);
        // 1 TOTALSIZE
        byteBuffer.putInt(storeSize);

        // 2 MAGICCODE
        byteBuffer.putInt(0);

        // 3 BODYCRC
        byteBuffer.putInt(0);

        // 4 FLAG
        int flag = message.getFlag();
        byteBuffer.putInt(flag);

        // 5 BODY
        byteBuffer.putInt(bodyLen);
        byteBuffer.put(body);

        // 6 properties
        byteBuffer.putShort(propertiesLength);
        byteBuffer.put(propertiesBytes);

        return byteBuffer.array();
    }

    public static Message decodeMessage(ByteBuffer byteBuffer) throws Exception {
        Message message = new Message();

        // 1 TOTALSIZE
        byteBuffer.getInt();

        // 2 MAGICCODE
        byteBuffer.getInt();

        // 3 BODYCRC
        byteBuffer.getInt();

        // 4 FLAG
        int flag = byteBuffer.getInt();
        message.setFlag(flag);

        // 5 BODY
        int bodyLen = byteBuffer.getInt();
        byte[] body = new byte[bodyLen];
        byteBuffer.get(body);
        message.setBody(body);

        // 6 properties
        short propertiesLen = byteBuffer.getShort();
        byte[] propertiesBytes = new byte[propertiesLen];
        byteBuffer.get(propertiesBytes);
        // opt16: parse directly from bytes; skip the intermediate String allocation.
        message.setProperties(bytes2messageProperties(propertiesBytes, 0, propertiesLen));

        return message;
    }

    public static byte[] encodeMessages(List<Message> messages) {
        //TO DO refactor, accumulate in one buffer, avoid copies
        List<byte[]> encodedMessages = new ArrayList<>(messages.size());
        int allSize = 0;
        for (Message message : messages) {
            byte[] tmp = encodeMessage(message);
            encodedMessages.add(tmp);
            allSize += tmp.length;
        }
        byte[] allBytes = new byte[allSize];
        int pos = 0;
        for (byte[] bytes : encodedMessages) {
            System.arraycopy(bytes, 0, allBytes, pos, bytes.length);
            pos += bytes.length;
        }
        return allBytes;
    }

    public static List<Message> decodeMessages(ByteBuffer byteBuffer) throws Exception {
        //TO DO add a callback for processing,  avoid creating lists
        List<Message> msgs = new ArrayList<>();
        while (byteBuffer.hasRemaining()) {
            Message msg = decodeMessage(byteBuffer);
            msgs.add(msg);
        }
        return msgs;
    }

    public static void decodeMessage(MessageExt messageExt, List<MessageExt> list) throws Exception {
        List<Message> messages = MessageDecoder.decodeMessages(ByteBuffer.wrap(messageExt.getBody()));
        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            MessageClientExt messageClientExt = new MessageClientExt();
            messageClientExt.setTopic(messageExt.getTopic());
            messageClientExt.setQueueOffset(messageExt.getQueueOffset() + i);
            messageClientExt.setQueueId(messageExt.getQueueId());
            messageClientExt.setFlag(message.getFlag());
            MessageAccessor.setProperties(messageClientExt, message.getProperties());
            messageClientExt.setBody(message.getBody());
            messageClientExt.setStoreHost(messageExt.getStoreHost());
            messageClientExt.setBornHost(messageExt.getBornHost());
            messageClientExt.setBornTimestamp(messageExt.getBornTimestamp());
            messageClientExt.setStoreTimestamp(messageExt.getStoreTimestamp());
            messageClientExt.setSysFlag(messageExt.getSysFlag());
            messageClientExt.setCommitLogOffset(messageExt.getCommitLogOffset());
            messageClientExt.setWaitStoreMsgOK(messageExt.isWaitStoreMsgOK());
            list.add(messageClientExt);
        }
    }

    public static int countInnerMsgNum(ByteBuffer buffer) {
        int count = 0;
        while (buffer.hasRemaining()) {
            count++;
            int currPos = buffer.position();
            int size = buffer.getInt();
            buffer.position(currPos + size);
        }
        return count;
    }
}
