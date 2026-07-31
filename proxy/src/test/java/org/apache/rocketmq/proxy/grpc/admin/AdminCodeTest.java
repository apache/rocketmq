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

package org.apache.rocketmq.proxy.grpc.admin;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class AdminCodeTest {

    @Test
    public void testCodeValues() {
        assertEquals(1, AdminCode.OK.getCode());
        assertEquals(2, AdminCode.INTERNAL_ERROR.getCode());
        assertEquals(3, AdminCode.BAD_REQUEST.getCode());
        assertEquals(4, AdminCode.UNAUTHORIZED.getCode());
        assertEquals(5, AdminCode.FORBIDDEN.getCode());
        assertEquals(6, AdminCode.NOT_FOUND.getCode());
        assertEquals(7, AdminCode.TOO_MANY_REQUESTS.getCode());
        assertEquals(8, AdminCode.CONFLICT.getCode());
    }

    @Test
    public void testDescriptions() {
        assertEquals("OK", AdminCode.OK.getDescription());
        assertEquals("Internal error", AdminCode.INTERNAL_ERROR.getDescription());
        assertEquals("Bad request", AdminCode.BAD_REQUEST.getDescription());
        assertEquals("Unauthorized", AdminCode.UNAUTHORIZED.getDescription());
        assertEquals("Forbidden", AdminCode.FORBIDDEN.getDescription());
        assertEquals("Not found", AdminCode.NOT_FOUND.getDescription());
        assertEquals("Too many requests", AdminCode.TOO_MANY_REQUESTS.getDescription());
        assertEquals("Conflict", AdminCode.CONFLICT.getDescription());
    }

    @Test
    public void testFromCode() {
        for (AdminCode adminCode : AdminCode.values()) {
            assertSame(adminCode, AdminCode.fromCode(adminCode.getCode()));
        }
    }

    @Test
    public void testFromCodeUnknown() {
        assertNull(AdminCode.fromCode(0));
        assertNull(AdminCode.fromCode(-1));
        assertNull(AdminCode.fromCode(999));
    }

    @Test
    public void testEnumCompleteness() {
        assertEquals(8, AdminCode.values().length);
        for (AdminCode adminCode : AdminCode.values()) {
            assertNotNull(adminCode.getDescription());
        }
    }
}
