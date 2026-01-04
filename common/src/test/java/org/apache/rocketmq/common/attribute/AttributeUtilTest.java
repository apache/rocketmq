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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AttributeUtilTest {

    private Map<String, Attribute> allAttributes;
    private ImmutableMap<String, String> currentAttributes;

    @Before
    public void setUp() {
        allAttributes = new HashMap<>();
        allAttributes.put("attr1", new TestAttribute("value1", true, value -> true));
        allAttributes.put("attr2", new TestAttribute("value2", true, value -> true));
        allAttributes.put("attr3", new TestAttribute("value3", true, value -> value.equals("valid")));

        currentAttributes = ImmutableMap.of("attr1", "value1", "attr2", "value2");
    }

    @Test
    public void alterCurrentAttributes_CreateMode_ShouldReturnOnlyAddedAttributes() {
        ImmutableMap<String, String> newAttributes = ImmutableMap.of("+attr1", "new_value1", "+attr2", "value2", "+attr3", "value3");

        Map<String, String> result = AttributeUtil.alterCurrentAttributes(true, allAttributes, currentAttributes, newAttributes);

        assertEquals(3, result.size());
        assertTrue(result.containsKey("attr1"));
        assertEquals("new_value1", result.get("attr1"));
        assertTrue(result.containsKey("attr3"));
        assertEquals("value3", result.get("attr3"));
        assertTrue(result.containsKey("attr2"));
    }

    @Test(expected = RuntimeException.class)
    public void alterCurrentAttributes_CreateMode_AddNonAddableAttribute_ShouldThrowException() {
        ImmutableMap<String, String> newAttributes = ImmutableMap.of("attr1", "value1");
        AttributeUtil.alterCurrentAttributes(true, allAttributes, currentAttributes, newAttributes);
    }

    @Test
    public void alterCurrentAttributes_UpdateMode_ShouldReturnUpdatedAndAddedAttributes() {
        ImmutableMap<String, String> newAttributes = ImmutableMap.of("+attr1", "new_value1", "-attr2", "value2", "+attr3", "value3");

        Map<String, String> result = AttributeUtil.alterCurrentAttributes(false, allAttributes, currentAttributes, newAttributes);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("attr1"));
        assertEquals("new_value1", result.get("attr1"));
        assertTrue(result.containsKey("attr3"));
        assertEquals("value3", result.get("attr3"));
        assertFalse(result.containsKey("attr2"));
    }

    @Test(expected = RuntimeException.class)
    public void alterCurrentAttributes_UpdateMode_DeleteNonExistentAttribute_ShouldThrowException() {
        ImmutableMap<String, String> newAttributes = ImmutableMap.of("-attr4", "value4");
        AttributeUtil.alterCurrentAttributes(false, allAttributes, currentAttributes, newAttributes);
    }

    @Test(expected = RuntimeException.class)
    public void alterCurrentAttributes_UpdateMode_WrongFormatKey_ShouldThrowException() {
        ImmutableMap<String, String> newAttributes = ImmutableMap.of("attr1", "+value1");
        AttributeUtil.alterCurrentAttributes(false, allAttributes, currentAttributes, newAttributes);
    }

    @Test(expected = RuntimeException.class)
    public void alterCurrentAttributes_UnsupportedKey_ShouldThrowException() {
        ImmutableMap<String, String> newAttributes = ImmutableMap.of("unsupported_attr", "value");
        AttributeUtil.alterCurrentAttributes(false, allAttributes, currentAttributes, newAttributes);
    }

    @Test(expected = RuntimeException.class)
    public void alterCurrentAttributes_AttemptToUpdateUnchangeableAttribute_ShouldThrowException() {
        ImmutableMap<String, String> newAttributes = ImmutableMap.of("attr2", "new_value2");
        AttributeUtil.alterCurrentAttributes(false, allAttributes, currentAttributes, newAttributes);
    }

    private static class TestAttribute extends Attribute {
        private final AttributeValidator validator;

        public TestAttribute(String name, boolean changeable, AttributeValidator validator) {
            super(name, changeable);
            this.validator = validator;
        }

        @Override
        public void verify(String value) {
            validator.validate(value);
        }
    }

    private interface AttributeValidator {
        boolean validate(String value);
    }
}
