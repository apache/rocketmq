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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DataConverterTest {

    @Test
    public void testSetBitChangesOnlyTheTargetBit() {
        int bitmap = 0b1010;

        assertThat(DataConverter.setBit(bitmap, 2, true)).isEqualTo(0b1110);
        assertThat(DataConverter.setBit(bitmap, 1, false)).isEqualTo(0b1000);
        assertThat(DataConverter.setBit(bitmap, 1, true)).isEqualTo(bitmap);
        assertThat(DataConverter.setBit(bitmap, 2, false)).isEqualTo(bitmap);
    }

    @Test
    public void testGetBitReadsOnlyTheTargetBit() {
        int bitmap = 0b1010;

        assertThat(DataConverter.getBit(bitmap, 1)).isTrue();
        assertThat(DataConverter.getBit(bitmap, 2)).isFalse();
    }

    @Test
    public void testLowestBit() {
        assertThat(DataConverter.setBit(0b10, 0, true)).isEqualTo(0b11);
        assertThat(DataConverter.setBit(0b11, 0, false)).isEqualTo(0b10);
        assertThat(DataConverter.getBit(0b11, 0)).isTrue();
        assertThat(DataConverter.getBit(0b10, 0)).isFalse();
    }

    @Test
    public void testSignBit() {
        int bitmapWithSignBit = Integer.MIN_VALUE | 1;

        assertThat(DataConverter.setBit(1, 31, true)).isEqualTo(bitmapWithSignBit);
        assertThat(DataConverter.setBit(bitmapWithSignBit, 31, false)).isEqualTo(1);
        assertThat(DataConverter.getBit(bitmapWithSignBit, 31)).isTrue();
        assertThat(DataConverter.getBit(1, 31)).isFalse();
        assertThat(DataConverter.getBit(Integer.MIN_VALUE, 0)).isFalse();
    }
}
