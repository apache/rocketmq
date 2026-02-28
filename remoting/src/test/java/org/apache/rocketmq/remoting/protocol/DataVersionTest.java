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

package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson2.JSON;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataVersionTest {

    @Test
    public void testEquals() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setTimestamp(dataVersion.getTimestamp());
        assertEquals(dataVersion, other);
    }

    @Test
    public void testEquals_falseWhenCounterDifferent() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setCounter(new AtomicLong(1L));
        other.setTimestamp(dataVersion.getTimestamp());
        assertNotEquals(dataVersion, other);
    }

    @Test
    public void testEquals_falseWhenCounterDifferent2() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setCounter(null);
        other.setTimestamp(dataVersion.getTimestamp());
        assertNotEquals(dataVersion, other);
    }

    @Test
    public void testEquals_falseWhenCounterDifferent3() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setCounter(null);
        DataVersion other = new DataVersion();
        other.setTimestamp(dataVersion.getTimestamp());
        assertNotEquals(dataVersion, other);
    }

    @Test
    public void testEquals_trueWhenCountersBothNull() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setCounter(null);
        DataVersion other = new DataVersion();
        other.setCounter(null);
        other.setTimestamp(dataVersion.getTimestamp());
        assertEquals(dataVersion, other);
    }

    @Test
    public void testEncode() {
        DataVersion dataVersion = new DataVersion();
        assertTrue(dataVersion.encode().length > 0);
        assertNotNull(dataVersion.toJson());
    }

    @Test
    public void testJsonSerializationAndDeserialization() {
        DataVersion expected = new DataVersion();
        expected.setCounter(new AtomicLong(Long.MAX_VALUE));
        expected.setTimestamp(expected.getTimestamp());
        String jsonStr = expected.toJson();
        assertNotNull(jsonStr);
        DataVersion actual = JSON.parseObject(jsonStr, DataVersion.class);
        assertNotNull(actual);
        assertEquals(expected.getTimestamp(), actual.getTimestamp());
    }
}
