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

package org.apache.rocketmq.remoting.impl.command;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map.Entry;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.api.exception.RemoteCodecException;

public class CodecHelper {
    //ProtocolType + TotalLength + RequestId + SerializeType + TrafficType + CodeLength + RemarkLength + PropertiesSize + ParameterLength
    public final static int MIN_PROTOCOL_LEN = 1 + 4 + 4 + 1 + 1 + 2 + 2 + 2 + 4;
    public final static char PROPERTY_SEPARATOR = '\n';
    public final static Charset REMOTING_CHARSET = Charset.forName("UTF-8");

    public final static int CODE_MAX_LEN = 512;
    public final static int PARAMETER_MAX_LEN = 33554432;
    public final static int BODY_MAX_LEN = 33554432;
    public final static int PACKET_MAX_LEN = 33554432;

    public static ByteBuffer encodeHeader(final RemotingCommand command, final int parameterLength,
        final int extraPayload) {
        byte[] code = command.opCode().getBytes(REMOTING_CHARSET);
        int codeLength = code.length;

        byte[] remark = command.remark().getBytes(REMOTING_CHARSET);
        int remarkLength = remark.length;

        byte[][] props = null;
        int propsLength = 0;
        StringBuilder sb = new StringBuilder();
        if (!command.properties().isEmpty()) {
            props = new byte[command.properties().size()][];
            int i = 0;
            for (Entry<String, String> next : command.properties().entrySet()) {
                sb.setLength(0);
                sb.append(next.getKey());
                sb.append(PROPERTY_SEPARATOR);
                sb.append(next.getValue());

                props[i] = sb.toString().getBytes(REMOTING_CHARSET);

                propsLength += 2;
                propsLength += props[i].length;
                i++;
            }
        }

        int totalLength = MIN_PROTOCOL_LEN - 1 - 4
            + codeLength
            + remarkLength
            + propsLength
            + parameterLength
            + extraPayload;

        int headerLength = 1 + 4 + totalLength - parameterLength - extraPayload;

        ByteBuffer buf = ByteBuffer.allocate(headerLength);
        buf.put(command.protocolType());
        buf.putInt(totalLength);
        buf.putInt(command.requestID());
        buf.put(command.serializerType());
        buf.put((byte) command.trafficType().ordinal());

        buf.putShort((short) codeLength);
        if (codeLength > 0) {
            buf.put(code);
        }
        buf.putShort((short) remarkLength);
        if (remarkLength > 0) {
            buf.put(remark);
        }
        if (props != null) {
            buf.putShort((short) props.length);
            for (byte[] prop : props) {
                buf.putShort((short) prop.length);
                buf.put(prop);
            }
        } else {
            buf.putShort((short) 0);
        }

        buf.putInt(parameterLength);

        buf.flip();

        return buf;
    }

    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        RemotingCommandImpl cmd = new RemotingCommandImpl();
        int totalLength = byteBuffer.limit();
        cmd.requestID(byteBuffer.getInt());
        cmd.serializerType(byteBuffer.get());
        cmd.trafficType(TrafficType.parse(byteBuffer.get()));

        {
            short size = byteBuffer.getShort();
            if (size > 0 && size <= CODE_MAX_LEN) {
                byte[] bytes = new byte[size];
                byteBuffer.get(bytes);
                String str = new String(bytes, REMOTING_CHARSET);
                cmd.opCode(str);
            } else {
                throw new RemoteCodecException(String.format("Code length: %d over max limit: %d", size, CODE_MAX_LEN));
            }
        }

        {
            short size = byteBuffer.getShort();
            if (size > 0) {
                byte[] bytes = new byte[size];
                byteBuffer.get(bytes);
                String str = new String(bytes, REMOTING_CHARSET);
                cmd.remark(str);
            }
        }

        {
            short size = byteBuffer.getShort();
            if (size > 0) {
                for (int i = 0; i < size; i++) {
                    short length = byteBuffer.getShort();
                    if (length > 0) {
                        byte[] bytes = new byte[length];
                        byteBuffer.get(bytes);
                        String str = new String(bytes, REMOTING_CHARSET);
                        int index = str.indexOf(PROPERTY_SEPARATOR);
                        if (index > 0) {
                            String key = str.substring(0, index);
                            String value = str.substring(index + 1);
                            cmd.property(key, value);
                        }
                    }
                }
            }
        }

        {
            int size = byteBuffer.getInt();
            if (size > 0 && size <= PARAMETER_MAX_LEN) {
                byte[] bytes = new byte[size];
                byteBuffer.get(bytes);
                cmd.parameterBytes(bytes);
            } else if (size != 0) {
                throw new RemoteCodecException(String.format("Parameter size: %d over max limit: %d", size, PARAMETER_MAX_LEN));
            }
        }

        {
            int size = totalLength - byteBuffer.position();
            if (size > 0 && size <= BODY_MAX_LEN) {
                byte[] bytes = new byte[size];
                byteBuffer.get(bytes);
                cmd.extraPayload(bytes);
            } else if (size != 0) {
                throw new RemoteCodecException(String.format("Body size: %d over max limit: %d", size, BODY_MAX_LEN));
            }
        }

        return cmd;
    }
}
