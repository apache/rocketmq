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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Matches LiteTopic patterns using {@code __} as the segment separator. */
public final class LitePatternMatcher {
    private static final String SEPARATOR = "__";

    private LitePatternMatcher() {
    }

    public static boolean matches(String pattern, String liteTopic) {
        if (!validate(pattern) || liteTopic == null || liteTopic.isEmpty()) {
            return false;
        }
        String[] patternSegments = pattern.split(SEPARATOR, -1);
        String[] topicSegments = liteTopic.split(SEPARATOR, -1);
        int topicIndex = 0;
        for (int i = 0; i < patternSegments.length; i++) {
            String segment = patternSegments[i];
            if ("**".equals(segment)) {
                return topicIndex < topicSegments.length;
            }
            if (topicIndex >= topicSegments.length || !("*".equals(segment) || segment.equals(topicSegments[topicIndex]))) {
                return false;
            }
            topicIndex++;
        }
        return topicIndex == topicSegments.length;
    }

    public static Set<String> expand(String pattern, Collection<String> candidates) {
        if (!validate(pattern) || candidates == null || candidates.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new LinkedHashSet<>();
        for (String candidate : candidates) {
            if (matches(pattern, candidate)) {
                result.add(candidate);
            }
        }
        return result;
    }

    public static boolean validate(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return false;
        }
        String[] segments = pattern.split(SEPARATOR, -1);
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i];
            if (segment.isEmpty() || segment.contains("*") && !"*".equals(segment) && !"**".equals(segment)) {
                return false;
            }
            if ("**".equals(segment) && i != segments.length - 1) {
                return false;
            }
        }
        return true;
    }
}
