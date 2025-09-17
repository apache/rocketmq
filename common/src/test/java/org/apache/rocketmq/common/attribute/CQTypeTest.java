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
package org.apache.rocketmq.common.attribute;

import org.apache.rocketmq.common.MixAll;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CQTypeTest {

    @Test
    public void testValues() {
        if (MixAll.isMac()) {
            return;
        }
        CQType[] values = CQType.values();
        assertEquals(3, values.length);
        assertEquals(CQType.SimpleCQ, values[0]);
        assertEquals(CQType.BatchCQ, values[1]);
        assertEquals(CQType.RocksDBCQ, values[2]);
    }

    @Test
    public void testValueOf() {
        if (MixAll.isMac()) {
            return;
        }
        assertEquals(CQType.SimpleCQ, CQType.valueOf("SimpleCQ"));
        assertEquals(CQType.BatchCQ, CQType.valueOf("BatchCQ"));
        assertEquals(CQType.RocksDBCQ, CQType.valueOf("RocksDBCQ"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueOf_InvalidName() {
        if (MixAll.isMac()) {
            return;
        }
        CQType.valueOf("InvalidCQ");
    }
}
