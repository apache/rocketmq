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

package org.apache.rocketmq.common.utils;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BinaryUtilTest {

    @Test
    public void testGenerateMd5FromString() {
        assertThat(BinaryUtil.generateMd5("hello")).isEqualTo("5D41402ABC4B2A76B9719D911017C592");
        assertThat(BinaryUtil.generateMd5("")).isEqualTo("D41D8CD98F00B204E9800998ECF8427E");
    }

    @Test
    public void testGenerateMd5FromBytesMatchesStringVersion() {
        byte[] content = "RocketMQ".getBytes(StandardCharsets.UTF_8);

        assertThat(BinaryUtil.generateMd5(content))
            .isEqualTo(BinaryUtil.generateMd5("RocketMQ"));
    }

    @Test
    public void testCalculateMd5ReturnsSixteenBytes() {
        assertThat(BinaryUtil.calculateMd5("hello".getBytes(StandardCharsets.UTF_8))).hasSize(16);
    }

    @Test
    public void testIsAsciiWithNullSubject() {
        assertThat(BinaryUtil.isAscii(null)).isFalse();
    }

    @Test
    public void testIsAsciiWithPrintableCharacters() {
        assertThat(BinaryUtil.isAscii("Hello RocketMQ ~!".getBytes(StandardCharsets.US_ASCII))).isTrue();
        assertThat(BinaryUtil.isAscii(new byte[0])).isTrue();
    }

    @Test
    public void testIsAsciiRejectsControlCharacters() {
        assertThat(BinaryUtil.isAscii(new byte[] {'a', '\t'})).isFalse();
        assertThat(BinaryUtil.isAscii(new byte[] {'a', '\n'})).isFalse();
    }

    @Test
    public void testIsAsciiRejectsNonAsciiBytes() {
        assertThat(BinaryUtil.isAscii("\u4e2d\u6587".getBytes(StandardCharsets.UTF_8))).isFalse();
        assertThat(BinaryUtil.isAscii(new byte[] {127})).isFalse();
    }
}
