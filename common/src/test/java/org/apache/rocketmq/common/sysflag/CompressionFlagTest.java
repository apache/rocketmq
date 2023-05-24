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

package org.apache.rocketmq.common.sysflag;

import org.apache.rocketmq.common.compression.CompressionType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionFlagTest {

    @Test
    public void testCompressionFlag() {
        int flag = 0;
        flag |= MessageSysFlag.COMPRESSED_FLAG;

        assertThat(MessageSysFlag.getCompressionType(flag)).isEqualTo(CompressionType.ZLIB);

        flag |= MessageSysFlag.COMPRESSION_LZ4_TYPE;
        assertThat(MessageSysFlag.getCompressionType(flag)).isEqualTo(CompressionType.LZ4);

        flag &= ~MessageSysFlag.COMPRESSION_TYPE_COMPARATOR;
        flag |= MessageSysFlag.COMPRESSION_ZSTD_TYPE;
        assertThat(MessageSysFlag.getCompressionType(flag)).isEqualTo(CompressionType.ZSTD);


        flag &= ~MessageSysFlag.COMPRESSION_TYPE_COMPARATOR;
        flag |= MessageSysFlag.COMPRESSION_ZLIB_TYPE;
        assertThat(MessageSysFlag.getCompressionType(flag)).isEqualTo(CompressionType.ZLIB);
    }

    @Test(expected = RuntimeException.class)
    public void testCompressionFlagNotMatch() {
        int flag = 0;
        flag |= MessageSysFlag.COMPRESSED_FLAG;
        flag |= 0x4 << 8;

        MessageSysFlag.getCompressionType(flag);
    }
}
