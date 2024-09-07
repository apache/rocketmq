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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.junit.Test;

public class ZlibCompressorTest {

    private static final String TEST_STRING = "The quick brown fox jumps over the lazy dog";

    @Test
    public void testCompressionAndDecompression() throws Exception {
        byte[] originalData = TEST_STRING.getBytes();
        ZlibCompressor compressor = new ZlibCompressor();
        byte[] compressedData = compressor.compress(originalData, 0);
        assertTrue("Compressed data should be bigger than original", compressedData.length > originalData.length);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data should match original", originalData, decompressedData);
    }

    @Test
    public void testCompressionFailureWithInvalidData() throws Exception {
        byte[] originalData = new byte[] {0, 1, 2, 3, 4};
        ZlibCompressor compressor = new ZlibCompressor();
        compressor.compress(originalData, 0);
    }

    @Test(expected = IOException.class)
    public void testDecompressionFailureWithInvalidData() throws Exception {
        byte[] compressedData = new byte[] {0, 1, 2, 3, 4};
        ZlibCompressor compressor = new ZlibCompressor();
        compressor.decompress(compressedData); // Invalid compressed data
    }
}
