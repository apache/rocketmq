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

package org.apache.rocketmq.acl.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class AuthorizationHeaderTest {

    private static final String AUTH_HEADER = "Signature Credential=1234567890/test, SignedHeaders=host, Signature=1234567890";
    private AuthorizationHeader authorizationHeader;

    @Before
    public void setUp() throws Exception {
        authorizationHeader = new AuthorizationHeader(AUTH_HEADER);
    }

    @Test
    public void testGetMethod() {
        Assert.assertEquals("Signature", authorizationHeader.getMethod());
    }

    @Test
    public void testGetAccessKey() {
        Assert.assertEquals("1234567890", authorizationHeader.getAccessKey());
    }

    @Test
    public void testGetSignedHeaders() {
        String[] expectedHeaders = {"host"};
        Assert.assertArrayEquals(expectedHeaders, authorizationHeader.getSignedHeaders());
    }

    @Test
    public void testGetSignature() {
        Assert.assertEquals("EjRWeJA=", authorizationHeader.getSignature());
    }

    @Test(expected = Exception.class)
    public void testInvalidAuthorizationHeader() throws Exception {
        new AuthorizationHeader("Invalid Header");
    }

    @Test(expected = Exception.class)
    public void testMalformedAuthorizationHeader() throws Exception {
        new AuthorizationHeader("Malformed, Header");
    }

}
