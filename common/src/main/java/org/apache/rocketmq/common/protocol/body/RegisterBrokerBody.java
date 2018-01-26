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

package org.apache.rocketmq.common.protocol.body;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterBrokerBody extends RemotingSerializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterBrokerBody.class);
    private TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
    private List<String> filterServerList = new ArrayList<String>();

    public byte[] gzipEncode(byte[] source) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DeflaterOutputStream outputStream = new DeflaterOutputStream(byteArrayOutputStream, new Deflater(Deflater.BEST_COMPRESSION));
        try {
            long start = System.currentTimeMillis();
            outputStream.write(source);
            outputStream.finish();

            long interval = System.currentTimeMillis() - start;
            if (interval > 50) {
                LOGGER.info("Compressing takes {}ms", interval);
            }

            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            LOGGER.error("Failed to compress RegisterBrokerBody object", e);
        }
        return null;
    }

    public static RegisterBrokerBody decode(byte[] data, int sourceLength) throws IOException {
        if (sourceLength > 0) {
            long start = System.currentTimeMillis();
            InflaterInputStream inflaterInputStream = new InflaterInputStream(new ByteArrayInputStream(data));
            byte[] readBytes = readBytes(inflaterInputStream, sourceLength);
            long interval = System.currentTimeMillis() - start;
            if (interval > 50) {
                LOGGER.info("Decompressing takes {}ms", interval);
            }
            return RegisterBrokerBody.decode(readBytes, RegisterBrokerBody.class);
        } else {
            return RegisterBrokerBody.decode(data, RegisterBrokerBody.class);
        }
    }

    private static byte[] readBytes(InflaterInputStream inflaterInputStream, int length) throws IOException {
        byte[] buffer = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int len = inflaterInputStream.read(buffer, bytesRead, length - bytesRead);
            if (len == -1) {
                throw new IOException("End of compressed data has reached");
            } else {
                bytesRead += len;
            }
        }
        return buffer;
    }

    private static int readInt(InflaterInputStream inflaterInputStream) throws IOException {
        byte[] buffer = readBytes(inflaterInputStream, 4);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        return byteBuffer.getInt();
    }

    public TopicConfigSerializeWrapper getTopicConfigSerializeWrapper() {
        return topicConfigSerializeWrapper;
    }

    public void setTopicConfigSerializeWrapper(TopicConfigSerializeWrapper topicConfigSerializeWrapper) {
        this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
    }

    public List<String> getFilterServerList() {
        return filterServerList;
    }

    public void setFilterServerList(List<String> filterServerList) {
        this.filterServerList = filterServerList;
    }
}
