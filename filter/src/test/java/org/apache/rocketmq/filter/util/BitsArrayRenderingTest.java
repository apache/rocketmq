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

package org.apache.rocketmq.filter.util;

import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BitsArrayRenderingTest {

    @Test
    public void testRenderingHasExactlyTheLogicalNumberOfBits() {
        for (int bitLength : new int[] {1, 7, 9, 10, 15, 17, 63, 65}) {
            BitsArray bits = BitsArray.create(bitLength);
            for (int i = 0; i < bits.byteLength(); i++) {
                bits.setByte(i, (byte) 0xFF);
            }
            char[] expected = new char[bitLength];
            Arrays.fill(expected, '1');
            assertEquals(new String(expected), bits.toString().replace("\n", ""));
        }
    }

    @Test
    public void testPaddingBitIsNotRendered() {
        BitsArray bits = BitsArray.create(7);
        bits.setByte(0, (byte) 0x80);
        assertEquals("0000000\n", bits.toString());
    }

    @Test
    public void testPartialByteKeepsHighToLowBitOrder() {
        BitsArray bits = BitsArray.create(9);
        bits.setBit(8, true);
        bits.setBit(0, true);
        assertEquals("100000001\n", bits.toString());
    }

    @Test
    public void testByteAlignedAndEmptyRenderingAreUnchanged() {
        assertEquals("1000000000000001\n", BitsArray.create(new byte[] {1, (byte) 0x80}).toString());
        assertEquals("", BitsArray.create(0).toString());
    }
}
