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
package org.apache.rocketmq.store.kv;

import org.apache.rocketmq.store.kv.CompactionLog.OffsetMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

public class OffsetMapTest {

    @Test
    public void testPutAndGet() throws Exception {
        OffsetMap offsetMap = new OffsetMap(0);     //min 100 entry
        offsetMap.put("abcde", 1);
        offsetMap.put("abc", 3);
        offsetMap.put("cde", 4);
        offsetMap.put("abcde", 9);
        assertEquals(offsetMap.get("abcde"), 9);
        assertEquals(offsetMap.get("cde"), 4);
        assertEquals(offsetMap.get("not_exist"), -1);
        assertEquals(offsetMap.getLastOffset(), 9);
    }

    @Test
    public void testFull() throws Exception {
        OffsetMap offsetMap = new OffsetMap(0);     //min 100 entry
        for (int i = 0; i < 100; i++) {
            offsetMap.put(String.valueOf(i), i);
        }

        assertEquals(offsetMap.get("66"), 66);
        assertNotEquals(offsetMap.get("55"), 56);
        assertEquals(offsetMap.getLastOffset(), 99);
        assertThrows(IllegalArgumentException.class, () -> offsetMap.put(String.valueOf(100), 100));
    }
}