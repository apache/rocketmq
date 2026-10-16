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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Bit-level utility methods, primarily used by Pop-mode ack tracking.
 *
 * <p>An {@code int} bitmask is used to track the ack state of up to 32 sub-messages
 * within a single Pop checkpoint (see {@code PopCheckPoint}).
 */
public class DataConverter {
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    /**
     * Convert a {@code long} to an 8-byte array (big-endian).
     */
    public static byte[] Long2Byte(Long v) {
        ByteBuffer tmp = ByteBuffer.allocate(8);
        tmp.putLong(v);
        return tmp.array();
    }

    /**
     * Set or clear the bit at {@code index} in an int bitmask.
     * <p>Uses {@code 1L} (long literal) to avoid signed-int overflow when {@code index == 31}.
     *
     * @param value the original bitmask
     * @param index the bit position (0-based, 0..31)
     * @param flag  {@code true} to set, {@code false} to clear
     * @return the updated bitmask
     */
    public static int setBit(int value, int index, boolean flag) {
        if (flag) {
            return (int) (value | (1L << index));
        } else {
            return (int) (value & ~(1L << index));
        }
    }

    /**
     * Test whether the bit at {@code index} is set in an int bitmask.
     *
     * @param value the bitmask
     * @param index the bit position (0-based, 0..31)
     * @return {@code true} if the bit is 1
     */
    public static boolean getBit(int value, int index) {
        return (value & (1L << index)) != 0;
    }
}
