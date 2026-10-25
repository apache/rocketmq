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

import java.util.Locale;
import java.util.Set;

public class SniHostnameMatcher {

    public static String findMatchingPattern(String hostname, Set<String> patterns) {
        if (hostname == null || hostname.isEmpty()) {
            return null;
        }

        String lowerHostname = hostname.toLowerCase(Locale.ROOT);

        // 1. Exact match
        if (patterns.contains(lowerHostname)) {
            return lowerHostname;
        }

        // 2. Wildcard match: try "*.suffix"
        String[] labels = lowerHostname.split("\\.");
        if (labels.length >= 2) {
            String wildcardCandidate = "*." + lowerHostname.substring(lowerHostname.indexOf('.') + 1);
            if (patterns.contains(wildcardCandidate)) {
                return wildcardCandidate;
            }
        }

        // 3. Bare-domain fallback: "example.com" matches "*.example.com"
        String bareDomainWildcard = "*." + lowerHostname;
        if (patterns.contains(bareDomainWildcard)) {
            return bareDomainWildcard;
        }

        return null;
    }
}
