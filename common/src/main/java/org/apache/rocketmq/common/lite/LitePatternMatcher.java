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
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * NATS-style wildcard pattern matcher for LiteTopic consumer subscriptions, operating in
 * child lite-topic name space (i.e. the {@code $liteTopic} segment of {@code %LMQ%$parent$liteTopic}).
 *
 * <p>Separator: {@code __} (double underscore). {@code $} is reserved as the LMQ separator
 * (see {@link LiteUtil#SEPARATOR}) and {@code .} is used in topic names/paths, so {@code __}
 * is used here to avoid conflicts.
 *
 * <p>Pattern tokens:
 * <ul>
 *   <li>{@code *} — matches exactly one {@code __}-delimited segment (allowed anywhere).</li>
 *   <li>{@code **} — matches one or more segments (must be the last segment, like NATS {@code >}).</li>
 * </ul>
 *
 * <p>Examples (parent topic = {@code order_events}):
 * <pre>
 *   pay__refund        matches pay__refund
 *   pay__*             matches pay__refund, pay__success
 *   *__refund          matches pay__refund, notify__refund
 *   pay__*__notify     matches pay__refund__notify, pay__success__notify
 *   **                 matches all
 *   pay__**            matches pay__refund, pay__refund__notify
 * </pre>
 *
 * <p>Matching is recursive segment-by-segment with no regex, so it is fast enough for the
 * dispatch hot path.
 */
public final class LitePatternMatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    /**
     * Segment separator for patterns and lite-topic names. Distinct from {@link LiteUtil#SEPARATOR}
     * (which is the LMQ {@code $} separator and must not appear in lite-topic child names).
     */
    public static final String SEPARATOR = "__";

    private static final String SINGLE = "*";
    private static final String DOUBLE = "**";

    private LitePatternMatcher() {
    }

    /**
     * Validate the syntax of a pattern.
     *
     * <p>Rules:
     * <ul>
     *   <li>non-null and non-empty;</li>
     *   <li>no empty segment (rejects leading/trailing/duplicate {@code __}, e.g. {@code pay__},
     *       {@code __pay}, {@code pay____x});</li>
     *   <li>{@code **} only allowed as the last segment;</li>
     *   <li>{@code *} is a whole-segment token; a segment mixing {@code *} with other chars
     *       (e.g. {@code pay*}) is invalid.</li>
     * </ul>
     *
     * @param pattern the pattern to validate
     * @return true if the pattern is syntactically valid
     */
    public static boolean validate(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return false;
        }
        String[] segments = pattern.split(SEPARATOR, -1);
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i];
            if (segment.isEmpty()) {
                return false;
            }
            if (DOUBLE.equals(segment)) {
                // ** only allowed as the last segment
                if (i != segments.length - 1) {
                    return false;
                }
            } else if (SINGLE.equals(segment)) {
                // * is allowed anywhere
            } else if (segment.indexOf('*') >= 0) {
                // a segment mixing * with other chars is invalid
                return false;
            }
            // otherwise a literal segment, always valid
        }
        return true;
    }

    /**
     * Test whether a lite-topic name matches a pattern. Both are split on {@code __}.
     *
     * @param pattern   the pattern, should be {@link #validate(String)} valid
     * @param liteTopic the candidate lite-topic child name
     * @return true if the lite-topic matches the pattern
     */
    public static boolean matches(String pattern, String liteTopic) {
        if (!validate(pattern) || liteTopic == null || liteTopic.isEmpty()) {
            return false;
        }
        String[] p = pattern.split(SEPARATOR, -1);
        String[] t = liteTopic.split(SEPARATOR, -1);
        return matchSeg(p, 0, t, 0);
    }

    /**
     * Recursive segment matcher.
     *
     * <p>Invariants enforced by {@link #validate(String)}: no empty segments, {@code **} only at
     * the last index, {@code *} is a whole segment.
     */
    private static boolean matchSeg(String[] p, int pi, String[] t, int ti) {
        if (pi == p.length) {
            // pattern exhausted: topic must also be exhausted
            return ti == t.length;
        }
        String segment = p[pi];
        if (DOUBLE.equals(segment)) {
            // ** matches one or more remaining segments to the end (validated to be last)
            return ti < t.length;
        }
        if (SINGLE.equals(segment)) {
            // * matches exactly one segment
            return ti < t.length && matchSeg(p, pi + 1, t, ti + 1);
        }
        // literal segment
        return ti < t.length && segment.equals(t[ti]) && matchSeg(p, pi + 1, t, ti + 1);
    }

    /**
     * Test whether a lite-topic name matches any of the given patterns. Invalid patterns are
     * skipped. This is a convenience over {@link #matches(String, String)} for the common
     * "does this candidate match any of the client's patterns" check on the dispatch hot path.
     *
     * @param patterns  the patterns to test
     * @param liteTopic the candidate lite-topic child name
     * @return true if the lite-topic matches at least one valid pattern
     */
    public static boolean matchesAny(Collection<String> patterns, String liteTopic) {
        if (patterns == null || patterns.isEmpty() || liteTopic == null || liteTopic.isEmpty()) {
            return false;
        }
        for (String pattern : patterns) {
            if (matches(pattern, liteTopic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Expand a set of patterns against a collection of candidate lite-topic names, returning the
     * union of candidates matched by any valid pattern. Invalid patterns are skipped (with a debug
     * log) rather than aborting the whole expansion, so one bad pattern does not kill a subscription.
     *
     * <p>Patterns and candidates are each pre-split once and reused across the inner loop to keep
     * the cost under the {@code <10ms / 1000 candidates} target.
     *
     * @param patterns   the patterns to expand
     * @param candidates the candidate lite-topic child names
     * @return the union of matched candidate names (never null)
     */
    public static Set<String> expand(Collection<String> patterns, Collection<String> candidates) {
        Set<String> matched = new HashSet<>();
        if (patterns == null || patterns.isEmpty() || candidates == null || candidates.isEmpty()) {
            return matched;
        }
        // Pre-split valid patterns once
        int count = 0;
        String[][] compiledPatterns = new String[patterns.size()][];
        for (String pattern : patterns) {
            if (!validate(pattern)) {
                LOGGER.debug("skip invalid wildcard pattern during expand: {}", pattern);
                continue;
            }
            compiledPatterns[count++] = pattern.split(SEPARATOR, -1);
        }
        if (count == 0) {
            return matched;
        }
        // Pre-split each candidate once and test against all compiled patterns
        for (String candidate : candidates) {
            if (candidate == null || candidate.isEmpty()) {
                continue;
            }
            String[] t = candidate.split(SEPARATOR, -1);
            for (int i = 0; i < count; i++) {
                if (matchSeg(compiledPatterns[i], 0, t, 0)) {
                    matched.add(candidate);
                    break;
                }
            }
        }
        return matched;
    }
}
