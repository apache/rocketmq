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

package org.apache.rocketmq.common.lite;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LitePatternMatcherTest {

    // ==================== validate ====================

    @Test
    public void validate_acceptsValidPatterns() {
        assertTrue(LitePatternMatcher.validate("pay__refund"));
        assertTrue(LitePatternMatcher.validate("pay__*"));
        assertTrue(LitePatternMatcher.validate("*__refund"));
        assertTrue(LitePatternMatcher.validate("pay__*__notify"));
        assertTrue(LitePatternMatcher.validate("**"));
        assertTrue(LitePatternMatcher.validate("pay__**"));
        assertTrue(LitePatternMatcher.validate("*"));
        assertTrue(LitePatternMatcher.validate("a__b__c"));
    }

    @Test
    public void validate_rejectsInvalidPatterns() {
        assertFalse(LitePatternMatcher.validate(null));
        assertFalse(LitePatternMatcher.validate(""));
        // empty segments
        assertFalse(LitePatternMatcher.validate("pay__"));
        assertFalse(LitePatternMatcher.validate("__pay"));
        assertFalse(LitePatternMatcher.validate("pay____refund"));
        // ** not at the end
        assertFalse(LitePatternMatcher.validate("**__pay"));
        assertFalse(LitePatternMatcher.validate("a__**__b"));
        // * mixed with other chars in a segment
        assertFalse(LitePatternMatcher.validate("pay*"));
        assertFalse(LitePatternMatcher.validate("pa*y"));
    }

    // ==================== matches — issue example rows ====================

    @Test
    public void matches_literalExact() {
        assertTrue(LitePatternMatcher.matches("pay__refund", "pay__refund"));
        assertFalse(LitePatternMatcher.matches("pay__refund", "pay__refund__notify"));
        assertFalse(LitePatternMatcher.matches("pay__refund", "notify__refund"));
    }

    @Test
    public void matches_singleLevelWildcard() {
        assertTrue(LitePatternMatcher.matches("pay__*", "pay__refund"));
        assertTrue(LitePatternMatcher.matches("pay__*", "pay__success"));
        assertFalse(LitePatternMatcher.matches("pay__*", "pay__refund__notify"));
    }

    @Test
    public void matches_singleLevelWildcardPrefix() {
        assertTrue(LitePatternMatcher.matches("*__refund", "pay__refund"));
        assertTrue(LitePatternMatcher.matches("*__refund", "notify__refund"));
        assertFalse(LitePatternMatcher.matches("*__refund", "pay__refund__notify"));
    }

    @Test
    public void matches_wildcardInMiddle() {
        assertTrue(LitePatternMatcher.matches("pay__*__notify", "pay__refund__notify"));
        assertTrue(LitePatternMatcher.matches("pay__*__notify", "pay__success__notify"));
        assertFalse(LitePatternMatcher.matches("pay__*__notify", "pay__refund"));
    }

    @Test
    public void matches_multiLevelAll() {
        assertTrue(LitePatternMatcher.matches("**", "pay"));
        assertTrue(LitePatternMatcher.matches("**", "pay__refund"));
        assertTrue(LitePatternMatcher.matches("**", "pay__refund__notify"));
        assertFalse(LitePatternMatcher.matches("**", ""));
    }

    @Test
    public void matches_multiLevelPrefix() {
        assertTrue(LitePatternMatcher.matches("pay__**", "pay__refund"));
        assertTrue(LitePatternMatcher.matches("pay__**", "pay__refund__notify"));
        assertFalse(LitePatternMatcher.matches("pay__**", "notify__refund"));
        // ** consumes one-or-more: pay__** must NOT match bare "pay"
        assertFalse(LitePatternMatcher.matches("pay__**", "pay"));
    }

    // ==================== matches — edge cases ====================

    @Test
    public void matches_invalidPatternNeverMatches() {
        assertFalse(LitePatternMatcher.matches("pay__", "pay__refund"));
        assertFalse(LitePatternMatcher.matches("**__pay", "pay"));
        assertFalse(LitePatternMatcher.matches("pay*", "payx"));
    }

    @Test
    public void matches_nullAndEmpty() {
        assertFalse(LitePatternMatcher.matches(null, "pay"));
        assertFalse(LitePatternMatcher.matches("pay", null));
        assertFalse(LitePatternMatcher.matches("pay", ""));
    }

    @Test
    public void matches_singleSegmentLiteralAndWildcard() {
        assertTrue(LitePatternMatcher.matches("pay", "pay"));
        assertFalse(LitePatternMatcher.matches("pay", "refund"));
        assertTrue(LitePatternMatcher.matches("*", "pay"));
        assertFalse(LitePatternMatcher.matches("*", "pay__refund"));
    }

    // ==================== matchesAny ====================

    @Test
    public void matchesAny_unionSemantics() {
        Set<String> patterns = new HashSet<>(Arrays.asList("pay__*", "notify__**"));
        assertTrue(LitePatternMatcher.matchesAny(patterns, "pay__refund"));
        assertTrue(LitePatternMatcher.matchesAny(patterns, "notify__refund__sms"));
        assertFalse(LitePatternMatcher.matchesAny(patterns, "order__created"));
    }

    @Test
    public void matchesAny_emptyAndNull() {
        assertFalse(LitePatternMatcher.matchesAny(null, "pay"));
        assertFalse(LitePatternMatcher.matchesAny(Collections.emptySet(), "pay"));
        assertFalse(LitePatternMatcher.matchesAny(Collections.singleton("pay__*"), null));
        assertFalse(LitePatternMatcher.matchesAny(Collections.singleton("pay__*"), ""));
    }

    @Test
    public void matchesAny_skipsInvalidPattern() {
        // an invalid pattern among valid ones must not break matching
        Set<String> patterns = new HashSet<>(Arrays.asList("pay__", "pay__*"));
        assertTrue(LitePatternMatcher.matchesAny(patterns, "pay__refund"));
        assertFalse(LitePatternMatcher.matchesAny(patterns, "order__x"));
    }

    // ==================== expand ====================

    @Test
    public void expand_unionOfMatched() {
        Set<String> patterns = new HashSet<>(Arrays.asList("pay__*", "notify__**"));
        Set<String> candidates = new HashSet<>(Arrays.asList(
            "pay__refund", "pay__success", "pay__refund__notify",
            "notify__refund", "notify__refund__sms", "order__created"));
        Set<String> matched = LitePatternMatcher.expand(patterns, candidates);
        // pay__* -> pay__refund, pay__success (NOT pay__refund__notify)
        // notify__** -> notify__refund, notify__refund__sms
        Set<String> expected = new HashSet<>(Arrays.asList(
            "pay__refund", "pay__success", "notify__refund", "notify__refund__sms"));
        assertEquals(expected, matched);
    }

    @Test
    public void expand_emptyInputs() {
        assertEquals(Collections.emptySet(),
            LitePatternMatcher.expand(Collections.emptySet(), Collections.singleton("pay__refund")));
        assertEquals(Collections.emptySet(),
            LitePatternMatcher.expand(Collections.singleton("pay__*"), Collections.emptySet()));
        assertEquals(Collections.emptySet(), LitePatternMatcher.expand(null, null));
    }

    @Test
    public void expand_skipsInvalidPattern() {
        Set<String> patterns = new HashSet<>(Arrays.asList("pay__", "pay__*"));
        Set<String> candidates = new HashSet<>(Arrays.asList("pay__refund", "order__x"));
        Set<String> matched = LitePatternMatcher.expand(patterns, candidates);
        assertEquals(Collections.singleton("pay__refund"), matched);
    }

    @Test
    public void expand_doubleStarMatchesAll() {
        Set<String> candidates = new HashSet<>(Arrays.asList("a", "a__b", "a__b__c"));
        Set<String> matched = LitePatternMatcher.expand(Collections.singleton("**"), candidates);
        assertEquals(candidates, matched);
    }
}
