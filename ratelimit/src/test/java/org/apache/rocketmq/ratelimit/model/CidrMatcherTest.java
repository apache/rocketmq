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
package org.apache.rocketmq.ratelimit.model;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CidrMatcherTest {

    @Test
    public void testMatches() {
        CidrMatcher cidrMatcher = new CidrMatcher("192.168.0.0/24");
        assertFalse(cidrMatcher.matches("192.168.1.1"));
        cidrMatcher = new CidrMatcher("192.168.0.0/16");
        assertTrue(cidrMatcher.matches("192.168.1.1"));
    }

    @Test
    public void testMatchesIpv6() {
        CidrMatcher cidrMatcher = new CidrMatcher("2001:0db8:1234:5678::/64");
        assertFalse(cidrMatcher.matches("2001:0db8:1234:5677::1234"));
        assertTrue(cidrMatcher.matches("2001:0db8:1234:5678::1234"));
    }

    @Test
    public void testMatchesNull() {
        CidrMatcher cidrMatcher = new CidrMatcher("2001:0db8:1234:5678::/64");
        assertFalse(cidrMatcher.matches(null));
        assertFalse(cidrMatcher.matches("df"));
    }
}