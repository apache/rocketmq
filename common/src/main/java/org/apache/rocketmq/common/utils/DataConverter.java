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

public class DataConverter {
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static byte[] Long2Byte(Long v) {
        ByteBuffer tmp = ByteBuffer.allocate(8);
        tmp.putLong(v);
        return tmp.array();
    }

    public static int setBit(int value, int index, boolean flag) {
        if (flag) {
            return (int) (value | (1L << index));
        } else {
            return (int) (value & ~(1L << index));
        }
    }

    public static boolean getBit(int value, int index) {
        return (value & (1L << index)) != 0;
    }
}
