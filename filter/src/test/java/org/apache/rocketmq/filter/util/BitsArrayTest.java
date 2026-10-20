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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BitsArrayTest {

    @Test
    public void testCreateWithBitLength() {
        BitsArray bits = BitsArray.create(10);
        assertEquals(10, bits.bitLength());
        assertEquals(2, bits.byteLength());
    }

    @Test
    public void testCreateWithBitLengthExactByte() {
        BitsArray bits = BitsArray.create(16);
        assertEquals(16, bits.bitLength());
        assertEquals(2, bits.byteLength());
    }

    @Test
    public void testCreateWithBytes() {
        byte[] data = new byte[] {(byte) 0xFF, (byte) 0x0F};
        BitsArray bits = BitsArray.create(data);
        assertEquals(16, bits.bitLength());
        assertArrayEquals(data, bits.bytes());
        assertNotSame(data, bits.bytes());
    }

    @Test
    public void testCreateWithBytesAndBitLength() {
        byte[] data = new byte[] {(byte) 0xAA, (byte) 0x03};
        BitsArray bits = BitsArray.create(data, 10);
        assertEquals(10, bits.bitLength());
        assertEquals(2, bits.byteLength());
        assertArrayEquals(data, bits.bytes());
        assertNotSame(data, bits.bytes());
        assertTrue(bits.getBit(9));
        assertThrows(IllegalArgumentException.class, () -> bits.getBit(10));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithNullBytes() {
        BitsArray.create((byte[]) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithEmptyBytes() {
        BitsArray.create(new byte[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithNullBytesAndBitLength() {
        BitsArray.create(null, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithZeroBitLength() {
        BitsArray.create(new byte[] {0x01}, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateWithBitLengthGreaterThanBytes() {
        BitsArray.create(new byte[] {0x01}, 9);
    }

    @Test
    public void testClonePreservesPartialByteLengthAndIndependentStorage() {
        for (int bitLength : new int[] {1, 7, 9, 10, 15, 17, 63, 64, 65}) {
            BitsArray original = BitsArray.create(bitLength);
            original.setBit(0, true);
            original.setBit(bitLength - 1, true);

            BitsArray copy = original.clone();

            assertEquals(bitLength, copy.bitLength());
            assertEquals(original.byteLength(), copy.byteLength());
            assertArrayEquals(original.bytes(), copy.bytes());
            assertNotSame(original.bytes(), copy.bytes());
            assertTrue(copy.getBit(bitLength - 1));
            assertThrows(IllegalArgumentException.class, () -> copy.getBit(bitLength));
            copy.setBit(0, false);
            assertTrue(original.getBit(0));
            assertFalse(copy.getBit(0));
        }
    }

    @Test
    public void testSetBitAndGetBit() {
        BitsArray bits = BitsArray.create(16);
        assertFalse(bits.getBit(0));
        assertFalse(bits.getBit(7));

        bits.setBit(0, true);
        assertTrue(bits.getBit(0));

        bits.setBit(7, true);
        assertTrue(bits.getBit(7));

        bits.setBit(0, false);
        assertFalse(bits.getBit(0));
    }

    @Test
    public void testSetByteAndGetByte() {
        BitsArray bits = BitsArray.create(16);
        bits.setByte(0, (byte) 0x55);
        assertEquals((byte) 0x55, bits.getByte(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBitOutOfRange() {
        BitsArray bits = BitsArray.create(8);
        bits.setBit(9, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBitOutOfRange() {
        BitsArray bits = BitsArray.create(8);
        bits.getBit(9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBitNegative() {
        BitsArray bits = BitsArray.create(8);
        bits.setBit(-1, true);
    }

    @Test(expected = RuntimeException.class)
    public void testCheckUninitialized() {
        BitsArray bits = BitsArray.create(0);
        bits.getBit(0);
    }

    @Test
    public void testXorTwoArrays() {
        BitsArray a = BitsArray.create(new byte[] {(byte) 0xFF});
        BitsArray b = BitsArray.create(new byte[] {(byte) 0x0F});
        a.xor(b);
        assertEquals((byte) 0xF0, a.getByte(0));
    }

    @Test
    public void testXorBitPosition() {
        BitsArray bits = BitsArray.create(8);
        bits.setBit(0, true);
        bits.xor(0, true);
        assertFalse(bits.getBit(0));

        bits.xor(0, true);
        assertTrue(bits.getBit(0));
    }

    @Test
    public void testOrTwoArrays() {
        BitsArray a = BitsArray.create(new byte[] {(byte) 0xA0});
        BitsArray b = BitsArray.create(new byte[] {(byte) 0x0A});
        a.or(b);
        assertEquals((byte) 0xAA, a.getByte(0));
    }

    @Test
    public void testOrBitPosition() {
        BitsArray bits = BitsArray.create(8);
        bits.setBit(3, false);
        bits.or(3, true);
        assertTrue(bits.getBit(3));
    }

    @Test
    public void testAndTwoArrays() {
        BitsArray a = BitsArray.create(new byte[] {(byte) 0xFF});
        BitsArray b = BitsArray.create(new byte[] {(byte) 0x0F});
        a.and(b);
        assertEquals((byte) 0x0F, a.getByte(0));
    }

    @Test
    public void testAndBitPosition() {
        BitsArray bits = BitsArray.create(8);
        bits.setBit(1, true);
        bits.and(1, false);
        assertFalse(bits.getBit(1));
    }

    @Test
    public void testNotBitPosition() {
        BitsArray bits = BitsArray.create(8);
        bits.setBit(5, false);
        bits.not(5);
        assertTrue(bits.getBit(5));
        bits.not(5);
        assertFalse(bits.getBit(5));
    }

    @Test
    public void testClone() {
        BitsArray original = BitsArray.create(new byte[] {(byte) 0xAB});
        BitsArray cloned = original.clone();
        assertEquals(original.bitLength(), cloned.bitLength());
        assertArrayEquals(original.bytes(), cloned.bytes());
        assertNotSame(original, cloned);
        assertNotSame(original.bytes(), cloned.bytes());
    }

    @Test
    public void testToString() {
        BitsArray bits = BitsArray.create(new byte[] {(byte) 0x03});
        String result = bits.toString();
        assertTrue(result.contains("1"));
        assertFalse(result.contains("null"));
    }

    @Test
    public void testLargeBitArray() {
        BitsArray bits = BitsArray.create(1024);
        assertEquals(1024, bits.bitLength());
        assertEquals(128, bits.byteLength());

        bits.setBit(1023, true);
        assertTrue(bits.getBit(1023));

        bits.setBit(0, true);
        assertTrue(bits.getBit(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetByteAtExactByteLengthThrowsIAE() {
        BitsArray bits = BitsArray.create(8);
        assertEquals(1, bits.byteLength());
        bits.getByte(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBitAtExactBitLengthThrowsIAE() {
        BitsArray bits = BitsArray.create(8);
        assertEquals(8, bits.bitLength());
        bits.getBit(8);
    }
}
