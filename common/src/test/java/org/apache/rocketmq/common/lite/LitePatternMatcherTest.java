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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LitePatternMatcherTest {
    @Test
    public void testMatchesExactAndSingleSegmentWildcard() {
        assertThat(LitePatternMatcher.matches("pay__refund", "pay__refund")).isTrue();
        assertThat(LitePatternMatcher.matches("pay__*", "pay__refund")).isTrue();
        assertThat(LitePatternMatcher.matches("pay__*", "pay__refund__notify")).isFalse();
        assertThat(LitePatternMatcher.matches("*__refund", "notify__refund")).isTrue();
    }

    @Test
    public void testMatchesMultiSegmentWildcardOnlyAtEnd() {
        assertThat(LitePatternMatcher.matches("**", "pay")).isTrue();
        assertThat(LitePatternMatcher.matches("**", "pay__refund__notify")).isTrue();
        assertThat(LitePatternMatcher.matches("pay__**", "pay__refund")).isTrue();
        assertThat(LitePatternMatcher.matches("pay__**", "pay")).isFalse();
        assertThat(LitePatternMatcher.matches("**__refund", "pay__refund")).isFalse();
    }

    @Test
    public void testExpandPreservesCandidateOrderAndRemovesDuplicates() {
        assertThat(LitePatternMatcher.expand("pay__*", Arrays.asList(
            "pay__refund", "notify__refund", "pay__success", "pay__refund")))
            .containsExactly("pay__refund", "pay__success");
    }

    @Test
    public void testRejectsInvalidPatterns() {
        assertThat(LitePatternMatcher.validate(null)).isFalse();
        assertThat(LitePatternMatcher.validate("pay____refund")).isFalse();
        assertThat(LitePatternMatcher.validate("pay__**__refund")).isFalse();
        assertThat(LitePatternMatcher.validate("pay__r*fund")).isFalse();
        assertThat(LitePatternMatcher.expand("pay__*", Collections.emptyList())).isEmpty();
    }
}
