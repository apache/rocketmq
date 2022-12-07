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

import junit.framework.TestCase;

public class RequestSourceTest extends TestCase {

    public void testIsValid() {
        assertEquals(4, RequestSource.values().length);

        assertTrue(RequestSource.isValid(-1));
        assertTrue(RequestSource.isValid(0));
        assertTrue(RequestSource.isValid(1));
        assertTrue(RequestSource.isValid(2));

        assertFalse(RequestSource.isValid(-2));
        assertFalse(RequestSource.isValid(3));
    }

    public void testParseInteger() {
        assertEquals(RequestSource.SDK, RequestSource.parseInteger(-1));
        assertEquals(RequestSource.PROXY_FOR_ORDER, RequestSource.parseInteger(0));
        assertEquals(RequestSource.PROXY_FOR_BROADCAST, RequestSource.parseInteger(1));
        assertEquals(RequestSource.PROXY_FOR_STREAM, RequestSource.parseInteger(2));

        assertEquals(RequestSource.SDK, RequestSource.parseInteger(-10));
        assertEquals(RequestSource.SDK, RequestSource.parseInteger(10));
    }
}
