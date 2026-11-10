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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * GZIP codec built only on the JDK, so a compressed body stays readable by any other
 * gzip aware producer or command line tool without pulling in another dependency.
 */
public class GzipCompressor implements Compressor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * GZIPOutputStream always builds its own Deflater with the default level and offers no
     * constructor taking a level, so the level requested by the caller is applied to the
     * inherited Deflater right after the super constructor has created it and before any
     * payload byte is deflated.
     */
    private static class LevelGzipOutputStream extends GZIPOutputStream {
        LevelGzipOutputStream(OutputStream out, int level) throws IOException {
            super(out);
            this.def.setLevel(level);
        }
    }

    @Override
    public byte[] compress(byte[] src, int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        GZIPOutputStream gzipOutputStream = new LevelGzipOutputStream(byteArrayOutputStream, level);
        try {
            gzipOutputStream.write(src);
            gzipOutputStream.finish();
            gzipOutputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            // Closing again is a no-op when the stream was already closed above, and it is
            // what releases the native Deflater when the write or finish step failed.
            try {
                gzipOutputStream.close();
            } catch (IOException ignored) {
            }
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
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            while (true) {
                int len = gzipInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                byteArrayOutputStream.write(uncompressData, 0, len);
            }
            byteArrayOutputStream.flush();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
            try {
                gzipInputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
        }

        return result;
    }
}
