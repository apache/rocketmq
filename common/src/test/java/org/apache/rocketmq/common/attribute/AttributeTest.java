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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AttributeTest {

    private Attribute attribute;

    @Before
    public void setUp() {
        attribute = new Attribute("testAttribute", true) {
            @Override
            public void verify(String value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Test
    public void testGetName_ShouldReturnCorrectName() {
        assertEquals("testAttribute", attribute.getName());
    }

    @Test
    public void testSetName_ShouldSetCorrectName() {
        attribute.setName("newTestAttribute");
        assertEquals("newTestAttribute", attribute.getName());
    }

    @Test
    public void testIsChangeable_ShouldReturnCorrectChangeableStatus() {
        assertTrue(attribute.isChangeable());
    }

    @Test
    public void testSetChangeable_ShouldSetCorrectChangeableStatus() {
        attribute.setChangeable(false);
        assertFalse(attribute.isChangeable());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testVerify_ShouldThrowUnsupportedOperationException() {
        attribute.verify("testValue");
    }

    @Test
    public void testEnumAttribute() {
        EnumAttribute enumAttribute = new EnumAttribute("enum.key", true, newHashSet("enum-1", "enum-2", "enum-3"), "enum-1");

        Assert.assertThrows(RuntimeException.class, () -> enumAttribute.verify(""));
        Assert.assertThrows(RuntimeException.class, () -> enumAttribute.verify("x"));
        Assert.assertThrows(RuntimeException.class, () -> enumAttribute.verify("enum-4"));

        enumAttribute.verify("enum-1");
        enumAttribute.verify("enum-2");
        enumAttribute.verify("enum-3");
    }

    @Test
    public void testLongRangeAttribute() {
        LongRangeAttribute longRangeAttribute = new LongRangeAttribute("long.range.key", true, 10, 20, 15);
        Assert.assertThrows(RuntimeException.class, () -> longRangeAttribute.verify(""));
        Assert.assertThrows(RuntimeException.class, () -> longRangeAttribute.verify(","));
        Assert.assertThrows(RuntimeException.class, () -> longRangeAttribute.verify("a"));
        Assert.assertThrows(RuntimeException.class, () -> longRangeAttribute.verify("-1"));
        Assert.assertThrows(RuntimeException.class, () -> longRangeAttribute.verify("21"));

        longRangeAttribute.verify("11");
        longRangeAttribute.verify("10");
        longRangeAttribute.verify("20");
    }

    @Test
    public void testBooleanAttribute() {
        BooleanAttribute booleanAttribute = new BooleanAttribute("bool.key", false, false);

        Assert.assertThrows(RuntimeException.class, () -> booleanAttribute.verify(""));
        Assert.assertThrows(RuntimeException.class, () -> booleanAttribute.verify("a"));
        Assert.assertThrows(RuntimeException.class, () -> booleanAttribute.verify(","));
        Assert.assertThrows(RuntimeException.class, () -> booleanAttribute.verify("checked"));
        Assert.assertThrows(RuntimeException.class, () -> booleanAttribute.verify("1"));
        Assert.assertThrows(RuntimeException.class, () -> booleanAttribute.verify("0"));
        Assert.assertThrows(RuntimeException.class, () -> booleanAttribute.verify("-1"));

        booleanAttribute.verify("true");
        booleanAttribute.verify("tRue");
        booleanAttribute.verify("false");
        booleanAttribute.verify("falSe");
    }
}
