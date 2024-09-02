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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class LongRangeAttributeTest {

    private LongRangeAttribute longRangeAttribute;

    @Before
    public void setUp() {
        longRangeAttribute = new LongRangeAttribute("testAttribute", true, 0, 100, 50);
    }

    @Test
    public void verify_ValidValue_NoExceptionThrown() {
        longRangeAttribute.verify("50");
    }

    @Test
    public void verify_MinValue_NoExceptionThrown() {
        longRangeAttribute.verify("0");
    }

    @Test
    public void verify_MaxValue_NoExceptionThrown() {
        longRangeAttribute.verify("100");
    }

    @Test
    public void verify_ValueLessThanMin_ThrowsRuntimeException() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> longRangeAttribute.verify("-1"));
        assertEquals("value is not in range(0, 100)", exception.getMessage());
    }

    @Test
    public void verify_ValueGreaterThanMax_ThrowsRuntimeException() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> longRangeAttribute.verify("101"));
        assertEquals("value is not in range(0, 100)", exception.getMessage());
    }

    @Test
    public void getDefaultValue_ReturnsDefaultValue() {
        assertEquals(50, longRangeAttribute.getDefaultValue());
    }
}
