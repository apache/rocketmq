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
package org.apache.rocketmq.proxy.service.cert;

import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SniHostnameMatcherTest {

    private Set<String> patterns(String... patterns) {
        Set<String> set = new HashSet<>();
        for (String p : patterns) {
            set.add(p);
        }
        return set;
    }

    @Test
    public void testExactMatch() {
        Set<String> p = patterns("*.example.com", "foo.example.com");
        assertEquals("foo.example.com", SniHostnameMatcher.findMatchingPattern("foo.example.com", p));
    }

    @Test
    public void testWildcardMatch() {
        Set<String> p = patterns("*.example.com");
        assertEquals("*.example.com", SniHostnameMatcher.findMatchingPattern("bar.example.com", p));
    }

    @Test
    public void testBareDomainFallback() {
        Set<String> p = patterns("*.example.com");
        assertEquals("*.example.com", SniHostnameMatcher.findMatchingPattern("example.com", p));
    }

    @Test
    public void testMultiLevelNoMatch() {
        Set<String> p = patterns("*.example.com");
        assertNull(SniHostnameMatcher.findMatchingPattern("a.b.example.com", p));
    }

    @Test
    public void testNoMatchFallsThrough() {
        Set<String> p = patterns("*.example.com");
        assertNull(SniHostnameMatcher.findMatchingPattern("bar.sample.org", p));
    }

    @Test
    public void testCaseInsensitive() {
        Set<String> p = patterns("*.example.com");
        assertEquals("*.example.com", SniHostnameMatcher.findMatchingPattern("FOO.EXAMPLE.COM", p));
    }

    @Test
    public void testNullHostname() {
        Set<String> p = patterns("*.example.com");
        assertNull(SniHostnameMatcher.findMatchingPattern(null, p));
    }

    @Test
    public void testEmptyHostname() {
        Set<String> p = patterns("*.example.com");
        assertNull(SniHostnameMatcher.findMatchingPattern("", p));
    }

    @Test
    public void testExactMatchPriorityOverWildcard() {
        Set<String> p = patterns("*.example.com", "foo.example.com");
        assertEquals("foo.example.com", SniHostnameMatcher.findMatchingPattern("foo.example.com", p));
    }

    @Test
    public void testMultipleDomains() {
        Set<String> p = patterns("*.example.com", "*.sample.org");
        assertEquals("*.example.com", SniHostnameMatcher.findMatchingPattern("foo.example.com", p));
        assertEquals("*.sample.org", SniHostnameMatcher.findMatchingPattern("bar.sample.org", p));
    }

    @Test
    public void testNoPatterns() {
        Set<String> p = patterns();
        assertNull(SniHostnameMatcher.findMatchingPattern("foo.example.com", p));
    }

    @Test
    public void testSingleLabelHostname() {
        Set<String> p = patterns("*.example.com");
        assertNull(SniHostnameMatcher.findMatchingPattern("localhost", p));
    }
}
