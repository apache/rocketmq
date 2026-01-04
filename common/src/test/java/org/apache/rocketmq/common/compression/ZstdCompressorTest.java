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
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class ZstdCompressorTest {

    @Test
    public void testCompressAndDecompress() throws IOException {
        byte[] originalData = "RocketMQ is awesome!".getBytes();
        ZstdCompressor compressor = new ZstdCompressor();
        byte[] compressedData = compressor.compress(originalData, 1);
        assertTrue("Compressed data should be bigger than original", compressedData.length > originalData.length);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data should match original data", originalData, decompressedData);
    }

    @Test
    public void testCompressWithInvalidData() throws IOException {
        byte[] invalidData = new byte[] {-1, -1, -1, -1};
        ZstdCompressor compressor = new ZstdCompressor();
        compressor.compress(invalidData, 1);
    }

    @Test(expected = IOException.class)
    public void testDecompressWithInvalidData() throws IOException {
        byte[] invalidData = new byte[] {-1, -1, -1, -1};
        ZstdCompressor compressor = new ZstdCompressor();
        compressor.decompress(invalidData);
    }

    @Test
    public void testCompressAndDecompressEmptyString() throws IOException {
        byte[] originalData = "".getBytes();
        ZstdCompressor compressor = new ZstdCompressor();
        byte[] compressedData = compressor.compress(originalData, 1);
        assertTrue("Compressed data for empty string should not be empty", compressedData.length > 0);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data for empty string should match original", originalData, decompressedData);
    }

    @Test
    public void testCompressAndDecompressLargeData() throws IOException {
        StringBuilder largeStringBuilder = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeStringBuilder.append("RocketMQ is awesome! ");
        }
        byte[] originalData = largeStringBuilder.toString().getBytes();

        ZstdCompressor compressor = new ZstdCompressor();
        byte[] compressedData = compressor.compress(originalData, 1);
        assertTrue("Compressed data for large data should be smaller than original", compressedData.length < originalData.length);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data for large data should match original", originalData, decompressedData);
    }
}
