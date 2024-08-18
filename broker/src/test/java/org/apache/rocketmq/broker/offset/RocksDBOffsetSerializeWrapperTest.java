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

package org.apache.rocketmq.broker.offset;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RocksDBOffsetSerializeWrapperTest {

    private RocksDBOffsetSerializeWrapper wrapper;

    @Before
    public void setUp() {
        wrapper = new RocksDBOffsetSerializeWrapper();
    }

    @Test
    public void testGetOffsetTable_ShouldReturnConcurrentHashMap() {
        ConcurrentMap<Integer, Long> offsetTable = wrapper.getOffsetTable();
        assertNotNull("The offsetTable should not be null", offsetTable);
        assertEquals("The offsetTable should be a ConcurrentMap", ConcurrentMap.class, offsetTable.getClass());
    }

    @Test
    public void testSetOffsetTable_ShouldSetTheOffsetTableCorrectly() {
        ConcurrentMap<Integer, Long> newOffsetTable = new ConcurrentHashMap<>();
        wrapper.setOffsetTable(newOffsetTable);
        ConcurrentMap<Integer, Long> offsetTable = wrapper.getOffsetTable();
        assertEquals("The offsetTable should be the same as the one set", newOffsetTable, offsetTable);
    }

}
