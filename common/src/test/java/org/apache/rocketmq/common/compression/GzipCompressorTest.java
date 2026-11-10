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
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class GzipCompressorTest {

    private static final String TEST_STRING = "The quick brown fox jumps over the lazy dog";

    @Test
    public void testCompressionAndDecompressionAtLevelBounds() throws Exception {
        byte[] originalData = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        GzipCompressor compressor = new GzipCompressor();
        for (int level : new int[] {0, 9}) {
            byte[] compressedData = compressor.compress(originalData, level);
            byte[] decompressedData = compressor.decompress(compressedData);
            assertArrayEquals("Decompressed data should match original at level " + level,
                originalData, decompressedData);
        }
    }

    @Test
    public void testCompressedDataIsLargerThanTinyInput() throws Exception {
        byte[] originalData = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        GzipCompressor compressor = new GzipCompressor();
        byte[] compressedData = compressor.compress(originalData, 0);
        assertTrue("Compressed data should be bigger than original", compressedData.length > originalData.length);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data should match original", originalData, decompressedData);
    }

    @Test
    public void testCompressionAndDecompressionOfEmptyBody() throws Exception {
        byte[] originalData = new byte[0];
        GzipCompressor compressor = new GzipCompressor();
        byte[] compressedData = compressor.compress(originalData, 5);
        assertTrue("Compressed data of an empty body should still carry the gzip frame",
            compressedData.length > 0);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data of an empty body should match original",
            originalData, decompressedData);
    }

    @Test
    public void testCompressionAndDecompressionOfLargeBody() throws Exception {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            builder.append("RocketMQ is awesome! ");
        }
        byte[] originalData = builder.toString().getBytes(StandardCharsets.UTF_8);

        GzipCompressor compressor = new GzipCompressor();
        byte[] compressedData = compressor.compress(originalData, 9);
        assertTrue("Compressed data for repetitive input should be smaller than original",
            compressedData.length < originalData.length);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data for repetitive input should match original",
            originalData, decompressedData);
    }

    @Test
    public void testDecompressIsRepeatable() throws Exception {
        byte[] originalData = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        GzipCompressor compressor = new GzipCompressor();
        byte[] compressedData = compressor.compress(originalData, 5);

        // The codec must not keep any state between calls: decompressing the same buffer twice
        // has to yield the same payload, exactly like a message being consumed by two clients.
        assertArrayEquals(originalData, compressor.decompress(compressedData));
        assertArrayEquals(originalData, compressor.decompress(compressedData));
    }

    @Test(expected = IOException.class)
    public void testDecompressionFailureWithInvalidData() throws Exception {
        byte[] compressedData = new byte[] {0, 1, 2, 3, 4};
        GzipCompressor compressor = new GzipCompressor();
        compressor.decompress(compressedData); // Invalid compressed data
    }

    @Test(expected = NullPointerException.class)
    public void testCompressionOfNullBodyFailsFast() throws Exception {
        GzipCompressor compressor = new GzipCompressor();
        compressor.compress(null, 5);
    }
}
