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

import org.junit.Assert;
import org.junit.Test;
import org.opentest4j.AssertionFailedError;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompressorFactoryTest {

    @Test
    public void testGetCompressor_ReturnsNonNull() {
        for (CompressionType type : CompressionType.values()) {
            Compressor compressor = CompressorFactory.getCompressor(type);
            Assert.assertNotNull("Compressor should not be null for type " + type, compressor);
        }
    }

    @Test
    public void testGetCompressor_ReturnsCorrectType() {
        for (CompressionType type : CompressionType.values()) {
            Compressor compressor = CompressorFactory.getCompressor(type);
            Assert.assertTrue("Compressor type mismatch for " + type,
                compressor instanceof Lz4Compressor && type == CompressionType.LZ4 ||
                    compressor instanceof ZstdCompressor && type == CompressionType.ZSTD ||
                    compressor instanceof ZlibCompressor && type == CompressionType.ZLIB);
        }
    }
}
