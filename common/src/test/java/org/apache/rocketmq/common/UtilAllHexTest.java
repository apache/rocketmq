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

package org.apache.rocketmq.common;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class UtilAllHexTest {
    @Test
    public void testMixedCaseHexRoundTrip() {
        assertArrayEquals(new byte[] {0, 15, (byte) 0xa5, (byte) 0xff},
            UtilAll.string2bytes("000Fa5FF"));
        assertArrayEquals(new byte[] {1, 35, 69, 103, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef},
            UtilAll.string2bytes("0123456789abcdef"));
    }

    @Test
    public void testRejectOddLength() {
        assertThrows(IllegalArgumentException.class, () -> UtilAll.string2bytes("ABC"));
        assertThrows(IllegalArgumentException.class, () -> UtilAll.string2bytes("0"));
    }

    @Test
    public void testRejectNonHexCharacters() {
        for (String value : new String[] {"G0", "0g", "-1", " 0", "\uff11\uff12"}) {
            assertThrows(value, IllegalArgumentException.class, () -> UtilAll.string2bytes(value));
        }
    }

    @Test
    public void testNullAndEmptyCompatibility() {
        assertNull(UtilAll.string2bytes(null));
        assertNull(UtilAll.string2bytes(""));
    }
}
