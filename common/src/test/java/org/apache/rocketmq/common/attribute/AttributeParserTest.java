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

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AttributeParserTest {

    @Test
    public void parseToMap_EmptyString_ReturnsEmptyMap() {
        String attributesModification = "";
        Map<String, String> result = AttributeParser.parseToMap(attributesModification);
        assertTrue(result.isEmpty());
    }

    @Test
    public void parseToMap_NullString_ReturnsEmptyMap() {
        String attributesModification = null;
        Map<String, String> result = AttributeParser.parseToMap(attributesModification);
        assertTrue(result.isEmpty());
    }

    @Test
    public void parseToMap_ValidAttributesModification_ReturnsExpectedMap() {
        String attributesModification = "+key1=value1,+key2=value2,-key3,+key4=value4";
        Map<String, String> result = AttributeParser.parseToMap(attributesModification);

        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put("+key1", "value1");
        expectedMap.put("+key2", "value2");
        expectedMap.put("-key3", "");
        expectedMap.put("+key4", "value4");

        assertEquals(expectedMap, result);
    }

    @Test(expected = RuntimeException.class)
    public void parseToMap_InvalidAddAttributeFormat_ThrowsRuntimeException() {
        String attributesModification = "+key1=value1,key2=value2,-key3,+key4=value4";
        AttributeParser.parseToMap(attributesModification);
    }

    @Test(expected = RuntimeException.class)
    public void parseToMap_InvalidDeleteAttributeFormat_ThrowsRuntimeException() {
        String attributesModification = "+key1=value1,+key2=value2,key3,+key4=value4";
        AttributeParser.parseToMap(attributesModification);
    }

    @Test(expected = RuntimeException.class)
    public void parseToMap_DuplicateKey_ThrowsRuntimeException() {
        String attributesModification = "+key1=value1,+key1=value2";
        AttributeParser.parseToMap(attributesModification);
    }

    @Test
    public void parseToString_EmptyMap_ReturnsEmptyString() {
        Map<String, String> attributes = new HashMap<>();
        String result = AttributeParser.parseToString(attributes);
        assertEquals("", result);
    }

    @Test
    public void parseToString_ValidAttributes_ReturnsExpectedString() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        attributes.put("key3", "");

        String result = AttributeParser.parseToString(attributes);
        String expectedString = "key1=value1,key2=value2,key3";
        assertEquals(expectedString, result);
    }

    @Test
    public void testParseToMap() {
        Assert.assertEquals(0, AttributeParser.parseToMap(null).size());
        AttributeParser.parseToMap("++=++");
        AttributeParser.parseToMap("--");
        Assert.assertThrows(RuntimeException.class, () -> AttributeParser.parseToMap("x"));
        Assert.assertThrows(RuntimeException.class, () -> AttributeParser.parseToMap("+"));
        Assert.assertThrows(RuntimeException.class, () -> AttributeParser.parseToMap("++"));
    }

    @Test
    public void testParseToString() {
        Assert.assertEquals("", AttributeParser.parseToString(null));
        Assert.assertEquals("", AttributeParser.parseToString(newHashMap()));
        HashMap<String, String> map = new HashMap<>();
        int addSize = 10;
        for (int i = 0; i < addSize; i++) {
            map.put("+add.key" + i, "value" + i);
        }
        int deleteSize = 10;
        for (int i = 0; i < deleteSize; i++) {
            map.put("-delete.key" + i, "");
        }
        Assert.assertEquals(addSize + deleteSize, AttributeParser.parseToString(map).split(",").length);
    }

    @Test
    public void testParseBetweenStringAndMapWithoutDistortion() {
        List<String> testCases = Arrays.asList("-a", "+a=b,+c=d,+z=z,+e=e", "+a=b,-d", "+a=b", "-a,-b");
        for (String testCase : testCases) {
            assertTrue(Maps.difference(AttributeParser.parseToMap(testCase), AttributeParser.parseToMap(parse(testCase))).areEqual());
        }
    }

    private String parse(String original) {
        Map<String, String> stringStringMap = AttributeParser.parseToMap(original);
        return AttributeParser.parseToString(stringStringMap);
    }
}
