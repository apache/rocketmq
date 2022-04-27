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
package org.apache.rocketmq.remoting.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

import io.netty.buffer.ByteBuf;

public class RocketMQSerializable {
    private static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    public static void writeStr(ByteBuf buf, boolean useShortLength, String str) {
        int lenIndex = buf.writerIndex();
        if (useShortLength) {
            buf.writeShort(0);
        } else {
            buf.writeInt(0);
        }
        int len = buf.writeCharSequence(str, StandardCharsets.UTF_8);
        if (useShortLength) {
            buf.setShort(lenIndex, len);
        } else {
            buf.setInt(lenIndex, len);
        }
    }

    private static String readStr(ByteBuf buf, boolean useShortLength, int limit) throws RemotingCommandException {
        int len = useShortLength ? buf.readShort() : buf.readInt();
        if (len == 0) {
            return null;
        }
        if (len > limit) {
            throw new RemotingCommandException("string length exceed limit:" + limit);
        }
        CharSequence cs = buf.readCharSequence(len, StandardCharsets.UTF_8);
        return cs == null ? null : cs.toString();
    }

    public static int rocketMQProtocolEncode(RemotingCommand cmd, ByteBuf out) {
        int beginIndex = out.writerIndex();
        // int code(~32767)
        out.writeShort(cmd.getCode());
        // LanguageCode language
        out.writeByte(cmd.getLanguage().getCode());
        // int version(~32767)
        out.writeShort(cmd.getVersion());
        // int opaque
        out.writeInt(cmd.getOpaque());
        // int flag
        out.writeInt(cmd.getFlag());
        // String remark
        String remark = cmd.getRemark();
        if (remark != null && !remark.isEmpty()) {
            writeStr(out, false, remark);
        } else {
            out.writeInt(0);
        }

        int mapLenIndex = out.writerIndex();
        out.writeInt(0);
        if (cmd.readCustomHeader() instanceof FastCodesHeader) {
            ((FastCodesHeader) cmd.readCustomHeader()).encode(out);
        }
        HashMap<String, String> map = cmd.getExtFields();
        if (map != null && !map.isEmpty()) {
            map.forEach((k, v) -> {
                if (k != null && v != null) {
                    writeStr(out, true, k);
                    writeStr(out, false, v);
                }
            });
        }
        out.setInt(mapLenIndex, out.writerIndex() - mapLenIndex - 4);
        return out.writerIndex() - beginIndex;
    }


    public static byte[] rocketMQProtocolEncode(RemotingCommand cmd) {
        // String remark
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if (cmd.getRemark() != null && cmd.getRemark().length() > 0) {
            remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
            remarkLen = remarkBytes.length;
        }

        // HashMap<String, String> extFields
        byte[] extFieldsBytes = null;
        int extLen = 0;
        if (cmd.getExtFields() != null && !cmd.getExtFields().isEmpty()) {
            extFieldsBytes = mapSerialize(cmd.getExtFields());
            extLen = extFieldsBytes.length;
        }

        int totalLen = calTotalLen(remarkLen, extLen);

        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
        // int code(~32767)
        headerBuffer.putShort((short) cmd.getCode());
        // LanguageCode language
        headerBuffer.put(cmd.getLanguage().getCode());
        // int version(~32767)
        headerBuffer.putShort((short) cmd.getVersion());
        // int opaque
        headerBuffer.putInt(cmd.getOpaque());
        // int flag
        headerBuffer.putInt(cmd.getFlag());
        // String remark
        if (remarkBytes != null) {
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(remarkBytes);
        } else {
            headerBuffer.putInt(0);
        }
        // HashMap<String, String> extFields;
        if (extFieldsBytes != null) {
            headerBuffer.putInt(extFieldsBytes.length);
            headerBuffer.put(extFieldsBytes);
        } else {
            headerBuffer.putInt(0);
        }

        return headerBuffer.array();
    }

    public static byte[] mapSerialize(HashMap<String, String> map) {
        // keySize+key+valSize+val
        if (null == map || map.isEmpty())
            return null;

        int totalLength = 0;
        int kvLength;
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                kvLength =
                    // keySize + Key
                    2 + entry.getKey().getBytes(CHARSET_UTF8).length
                        // valSize + val
                        + 4 + entry.getValue().getBytes(CHARSET_UTF8).length;
                totalLength += kvLength;
            }
        }

        ByteBuffer content = ByteBuffer.allocate(totalLength);
        byte[] key;
        byte[] val;
        it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                key = entry.getKey().getBytes(CHARSET_UTF8);
                val = entry.getValue().getBytes(CHARSET_UTF8);

                content.putShort((short) key.length);
                content.put(key);

                content.putInt(val.length);
                content.put(val);
            }
        }

        return content.array();
    }

    private static int calTotalLen(int remark, int ext) {
        // int code(~32767)
        int length = 2
            // LanguageCode language
            + 1
            // int version(~32767)
            + 2
            // int opaque
            + 4
            // int flag
            + 4
            // String remark
            + 4 + remark
            // HashMap<String, String> extFields
            + 4 + ext;

        return length;
    }

    public static RemotingCommand rocketMQProtocolDecode(final ByteBuf headerBuffer, int headerLen) throws RemotingCommandException {
        RemotingCommand cmd = new RemotingCommand();
        // int code(~32767)
        cmd.setCode(headerBuffer.readShort());
        // LanguageCode language
        cmd.setLanguage(LanguageCode.valueOf(headerBuffer.readByte()));
        // int version(~32767)
        cmd.setVersion(headerBuffer.readShort());
        // int opaque
        cmd.setOpaque(headerBuffer.readInt());
        // int flag
        cmd.setFlag(headerBuffer.readInt());
        // String remark
        cmd.setRemark(readStr(headerBuffer, false, headerLen));

        // HashMap<String, String> extFields
        int extFieldsLength = headerBuffer.readInt();
        if (extFieldsLength > 0) {
            if (extFieldsLength > headerLen) {
                throw new RemotingCommandException("RocketMQ protocol decoding failed, extFields length: " + extFieldsLength + ", but header length: " + headerLen);
            }
            cmd.setExtFields(mapDeserialize(headerBuffer, extFieldsLength));
        }
        return cmd;
    }

    public static HashMap<String, String> mapDeserialize(ByteBuf byteBuffer, int len) throws RemotingCommandException {

        HashMap<String, String> map = new HashMap<>();
        int endIndex = byteBuffer.readerIndex() + len;

        while (byteBuffer.readerIndex() < endIndex) {
            String k = readStr(byteBuffer, true, len);
            String v = readStr(byteBuffer, false, len);
            map.put(k, v);
        }
        return map;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
