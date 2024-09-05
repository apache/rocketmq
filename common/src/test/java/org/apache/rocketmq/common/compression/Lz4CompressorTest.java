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

public class Lz4CompressorTest {

    private static final String TEST_STRING = "The quick brown fox jumps over the lazy dog";

    @Test
    public void testCompressAndDecompress() throws Exception {
        byte[] originalData = TEST_STRING.getBytes();
        Compressor compressor = new Lz4Compressor();
        byte[] compressedData = compressor.compress(originalData, 1);
        assertTrue("Compressed data should be bigger than original", compressedData.length > originalData.length);

        byte[] decompressedData = compressor.decompress(compressedData);
        assertArrayEquals("Decompressed data should match original data", originalData, decompressedData);
    }

    @Test
    public void testCompressWithIOException() throws Exception {
        byte[] originalData = new byte[] {1, 2, 3};
        Compressor compressor = new Lz4Compressor();
        compressor.compress(originalData, 1);
    }

    @Test(expected = IOException.class)
    public void testDecompressWithIOException() throws Exception {
        byte[] compressedData = new byte[] {1, 2, 3};
        Compressor compressor = new Lz4Compressor();
        compressor.decompress(compressedData);
    }
}
