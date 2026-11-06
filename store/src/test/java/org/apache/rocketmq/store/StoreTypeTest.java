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
package org.apache.rocketmq.store;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class StoreTypeTest {

    @Test
    public void testFromStringNullEmptyOrBlank() {
        Assert.assertEquals(Collections.emptySet(), StoreType.fromString(null));
        Assert.assertEquals(Collections.emptySet(), StoreType.fromString(""));
        Assert.assertEquals(Collections.emptySet(), StoreType.fromString("   "));
    }

    @Test
    public void testFromStringSingleValue() {
        Assert.assertEquals(Collections.singleton(StoreType.DEFAULT), StoreType.fromString("default"));
        Assert.assertEquals(Collections.singleton(StoreType.DEFAULT_ROCKSDB), StoreType.fromString("defaultRocksDB"));
    }

    @Test
    public void testFromStringCaseInsensitiveAndSurroundingWhitespace() {
        Set<StoreType> expected = new HashSet<>();
        expected.add(StoreType.DEFAULT);
        expected.add(StoreType.DEFAULT_ROCKSDB);
        Assert.assertEquals(expected, StoreType.fromString(" DEFAULT ; DefaultRocksDB "));
    }

    @Test
    public void testFromStringDuplicatesDeduplicated() {
        Assert.assertEquals(Collections.singleton(StoreType.DEFAULT), StoreType.fromString("default;default;DEFAULT"));
    }

    @Test
    public void testFromStringEmptySegmentsSkipped() {
        Set<StoreType> expected = new HashSet<>();
        expected.add(StoreType.DEFAULT);
        expected.add(StoreType.DEFAULT_ROCKSDB);
        Assert.assertEquals(expected, StoreType.fromString("default;;defaultRocksDB"));
    }

    @Test
    public void testFromStringUnknownTokensDropped() {
        Assert.assertEquals(Collections.singleton(StoreType.DEFAULT), StoreType.fromString("default;unknown;foo_bar"));
    }

    @Test
    public void testFromStringMixedInput() {
        Set<StoreType> expected = new HashSet<>();
        expected.add(StoreType.DEFAULT);
        expected.add(StoreType.DEFAULT_ROCKSDB);
        Assert.assertEquals(expected, StoreType.fromString(" default ; DEFAULTROCKSDB;default;unknown;; "));
    }

    @Test
    public void testEachStoreTypeRoundTrips() {
        for (StoreType type : StoreType.values()) {
            Assert.assertEquals("fromString should round-trip " + type.getStoreType(),
                Collections.singleton(type), StoreType.fromString(type.getStoreType()));
        }
    }
}
