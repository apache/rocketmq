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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import org.junit.Before;
import org.junit.Test;

public class BooleanAttributeTest {

    private BooleanAttribute booleanAttribute;

    @Before
    public void setUp() {
        booleanAttribute = new BooleanAttribute("testAttribute", true, false);
    }

    @Test
    public void testVerify_ValidValue_NoExceptionThrown() {
        booleanAttribute.verify("true");
        booleanAttribute.verify("false");
    }

    @Test
    public void testVerify_InvalidValue_ExceptionThrown() {
        assertThrows(RuntimeException.class, () -> booleanAttribute.verify("invalid"));
        assertThrows(RuntimeException.class, () -> booleanAttribute.verify("1"));
        assertThrows(RuntimeException.class, () -> booleanAttribute.verify("0"));
        assertThrows(RuntimeException.class, () -> booleanAttribute.verify(""));
    }

    @Test
    public void testGetDefaultValue() {
        assertFalse(booleanAttribute.getDefaultValue());
    }
}
