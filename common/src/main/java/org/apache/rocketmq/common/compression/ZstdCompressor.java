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

package org.apache.rocketmq.common.compression;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class ZstdCompressor implements Compressor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    @Override
    public byte[] compress(byte[] src, int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        ZstdOutputStream outputStream = new ZstdOutputStream(byteArrayOutputStream, level);
        try {
            outputStream.write(src);
            outputStream.flush();
            outputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            log.error("Failed to compress data by zstd", e);
            throw e;
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignored) {
            }
        }
        return result;
    }

    @Override
    public byte[] decompress(byte[] src) throws IOException {
        byte[] result = src;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        ZstdInputStream zstdInputStream = new ZstdInputStream(byteArrayInputStream);
        ByteArrayOutputStream resultOutputStream = new ByteArrayOutputStream(src.length);

        try {
            while (true) {
                int len = zstdInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                resultOutputStream.write(uncompressData, 0, len);
            }
            resultOutputStream.flush();
            resultOutputStream.close();
            result = resultOutputStream.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                zstdInputStream.close();
                byteArrayInputStream.close();
            } catch (IOException e) {
                log.warn("Failed to close the zstd compress stream", e);
            }
        }

        return result;
    }
}