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

import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompressionTypeTest {

    @Test
    public void testCompressionTypeValues() {
        assertEquals(1, CompressionType.LZ4.getValue(), "LZ4 value should be 1");
        assertEquals(2, CompressionType.ZSTD.getValue(), "ZSTD value should be 2");
        assertEquals(3, CompressionType.ZLIB.getValue(), "ZLIB value should be 3");
    }

    @Test
    public void testCompressionTypeOf() {
        assertEquals(CompressionType.LZ4, CompressionType.of("LZ4"), "CompressionType.of(LZ4) should return LZ4");
        assertEquals(CompressionType.ZSTD, CompressionType.of("ZSTD"), "CompressionType.of(ZSTD) should return ZSTD");
        assertEquals(CompressionType.ZLIB, CompressionType.of("ZLIB"), "CompressionType.of(ZLIB) should return ZLIB");

        assertThrows(RuntimeException.class, () -> CompressionType.of("UNKNOWN"), "Unsupported compression type should throw RuntimeException");
    }

    @Test
    public void testCompressionTypeFindByValue() {
        assertEquals(CompressionType.LZ4, CompressionType.findByValue(1), "CompressionType.findByValue(1) should return LZ4");
        assertEquals(CompressionType.ZSTD, CompressionType.findByValue(2), "CompressionType.findByValue(2) should return ZSTD");
        assertEquals(CompressionType.ZLIB, CompressionType.findByValue(3), "CompressionType.findByValue(3) should return ZLIB");

        assertEquals(CompressionType.ZLIB, CompressionType.findByValue(0), "CompressionType.findByValue(0) should return ZLIB for backward compatibility");

        assertThrows(RuntimeException.class, () -> CompressionType.findByValue(99), "Invalid value should throw RuntimeException");
    }

    @Test
    public void testCompressionFlag() {
        assertEquals(MessageSysFlag.COMPRESSION_LZ4_TYPE, CompressionType.LZ4.getCompressionFlag(), "LZ4 compression flag is incorrect");
        assertEquals(MessageSysFlag.COMPRESSION_ZSTD_TYPE, CompressionType.ZSTD.getCompressionFlag(), "ZSTD compression flag is incorrect");
        assertEquals(MessageSysFlag.COMPRESSION_ZLIB_TYPE, CompressionType.ZLIB.getCompressionFlag(), "ZLIB compression flag is incorrect");
    }
}
