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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class CompressionTypeTest {

    @Test
    public void testCompressionTypeValues() {
        assertEquals(1, CompressionType.LZ4.getValue());
        assertEquals(2, CompressionType.ZSTD.getValue());
        assertEquals(3, CompressionType.ZLIB.getValue());
    }

    @Test
    public void testCompressionTypeOf() {
        assertEquals(CompressionType.LZ4, CompressionType.of("LZ4"));
        assertEquals(CompressionType.ZSTD, CompressionType.of("ZSTD"));
        assertEquals(CompressionType.ZLIB, CompressionType.of("ZLIB"));
        assertThrows(RuntimeException.class, () -> CompressionType.of("UNKNOWN"));
    }

    @Test
    public void testCompressionTypeFindByValue() {
        assertEquals(CompressionType.LZ4, CompressionType.findByValue(1));
        assertEquals(CompressionType.ZSTD, CompressionType.findByValue(2));
        assertEquals(CompressionType.ZLIB, CompressionType.findByValue(3));
        assertEquals(CompressionType.ZLIB, CompressionType.findByValue(0));
        assertThrows(RuntimeException.class, () -> CompressionType.findByValue(99));
    }

    @Test
    public void testCompressionFlag() {
        assertEquals(MessageSysFlag.COMPRESSION_LZ4_TYPE, CompressionType.LZ4.getCompressionFlag());
        assertEquals(MessageSysFlag.COMPRESSION_ZSTD_TYPE, CompressionType.ZSTD.getCompressionFlag());
        assertEquals(MessageSysFlag.COMPRESSION_ZLIB_TYPE, CompressionType.ZLIB.getCompressionFlag());
    }
}
