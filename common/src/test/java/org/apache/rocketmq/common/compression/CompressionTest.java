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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionTest {

    private int level;
    private Compressor zstd;
    private Compressor zlib;
    private Compressor lz4;

    @Before
    public void setUp() {
        level = 5;
        zstd = CompressorFactory.getCompressor(CompressionType.ZSTD);
        zlib = CompressorFactory.getCompressor(CompressionType.ZLIB);
        lz4 = CompressorFactory.getCompressor(CompressionType.LZ4);
    }

    @Test
    public void testCompressionZlib() throws IOException {
        assertThat(CompressionType.of("zlib")).isEqualTo(CompressionType.ZLIB);
        assertThat(CompressionType.of(" ZLiB ")).isEqualTo(CompressionType.ZLIB);
        assertThat(CompressionType.of("ZLIB")).isEqualTo(CompressionType.ZLIB);

        int randomKB = 4096;
        Random random = new Random();
        for (int i = 0; i < 5; ++i) {
            String message = RandomStringUtils.randomAlphanumeric(random.nextInt(randomKB) * 1024);
            byte[] srcBytes = message.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = zlib.compress(srcBytes, level);
            byte[] decompressed = zlib.decompress(compressed);
            // compression ratio may be negative for some random string data
            // assertThat(compressed.length).isLessThan(srcBytes.length);
            assertThat(decompressed).isEqualTo(srcBytes);
            assertThat(new String(decompressed)).isEqualTo(message);
        }
    }

    @Test
    public void testCompressionZstd() throws IOException {
        assertThat(CompressionType.of("zstd")).isEqualTo(CompressionType.ZSTD);
        assertThat(CompressionType.of("ZStd ")).isEqualTo(CompressionType.ZSTD);
        assertThat(CompressionType.of("ZSTD")).isEqualTo(CompressionType.ZSTD);

        int randomKB = 4096;
        Random random = new Random();
        for (int i = 0; i < 5; ++i) {
            String message = RandomStringUtils.randomAlphanumeric(random.nextInt(randomKB) * 1024);
            byte[] srcBytes = message.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = zstd.compress(srcBytes, level);
            byte[] decompressed = zstd.decompress(compressed);
            // compression ratio may be negative for some random string data
            // assertThat(compressed.length).isLessThan(srcBytes.length);
            assertThat(decompressed).isEqualTo(srcBytes);
            assertThat(new String(decompressed)).isEqualTo(message);
        }
    }

    @Test
    public void testCompressionLz4() throws IOException {
        assertThat(CompressionType.of("lz4")).isEqualTo(CompressionType.LZ4);

        int randomKB = 4096;
        Random random = new Random();
        for (int i = 0; i < 5; ++i) {
            String message = RandomStringUtils.randomAlphanumeric(random.nextInt(randomKB) * 1024);
            byte[] srcBytes = message.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = lz4.compress(srcBytes, level);
            byte[] decompressed = lz4.decompress(compressed);
            // compression ratio may be negative for some random string data
            // assertThat(compressed.length).isLessThan(srcBytes.length);
            assertThat(decompressed).isEqualTo(srcBytes);
            assertThat(new String(decompressed)).isEqualTo(message);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCompressionUnsupportedType() {
        CompressionType.of("snappy");
    }
}
