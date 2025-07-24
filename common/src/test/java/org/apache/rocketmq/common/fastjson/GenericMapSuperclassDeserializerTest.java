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
package org.apache.rocketmq.common.fastjson;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GenericMapSuperclassDeserializerTest {

    public static class CustomMap extends HashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    public static class IntKeyMap extends HashMap<Integer, String> {
        private static final long serialVersionUID = 1L;
    }

    @Test
    public void testBasicDeserialization() {
        JSON.registerIfAbsent(CustomMap.class, GenericMapSuperclassDeserializer.INSTANCE);
        String json = "{\"key1\":\"value1\",\"key2\":42,\"key3\":true}";
        CustomMap map = JSON.parseObject(json, CustomMap.class);

        assertNotNull(map);
        assertEquals(3, map.size());
        assertEquals("value1", map.get("key1"));
        assertEquals(42, map.get("key2"));
        assertEquals(true, map.get("key3"));
    }

    @Test
    public void testNestedObjects() {
        JSON.registerIfAbsent(CustomMap.class, GenericMapSuperclassDeserializer.INSTANCE);
        String json = "{\"simple\":\"value\",\"nested\":{\"inner\":123},\"array\":[1,2,3]}";
        CustomMap map = JSON.parseObject(json, CustomMap.class);

        assertNotNull(map);
        assertEquals(3, map.size());
        assertEquals("value", map.get("simple"));

        assertTrue(map.get("nested") instanceof Map);
        Map<?, ?> nestedMap = (Map<?, ?>) map.get("nested");
        assertEquals(123, nestedMap.get("inner"));

        assertTrue(map.get("array") instanceof java.util.List);
        java.util.List<?> array = (java.util.List<?>) map.get("array");
        assertEquals(3, array.size());
        assertEquals(1, array.get(0));
        assertEquals(2, array.get(1));
        assertEquals(3, array.get(2));
    }

    @Test
    public void testEmptyObject() {
        JSON.registerIfAbsent(CustomMap.class, GenericMapSuperclassDeserializer.INSTANCE);
        String json = "{}";
        CustomMap map = JSON.parseObject(json, CustomMap.class);

        assertNotNull(map);
        assertEquals(0, map.size());
    }

    @Test
    public void testNonStringKey() {
        JSON.registerIfAbsent(IntKeyMap.class, GenericMapSuperclassDeserializer.INSTANCE);
        String json = "{1:\"one\",2:\"two\",3:\"three\"}";
        IntKeyMap map = JSON.parseObject(json, IntKeyMap.class);

        assertNotNull(map);
        assertEquals(3, map.size());
        assertEquals("one", map.get(1));
        assertEquals("two", map.get(2));
        assertEquals("three", map.get(3));
    }

    @Test(expected = JSONException.class)
    public void testMalformedJson() {
        JSON.registerIfAbsent(CustomMap.class, GenericMapSuperclassDeserializer.INSTANCE);
        String json = "{\"key\":\"missing closing brace\"";
        JSON.parseObject(json, CustomMap.class);
    }
}
