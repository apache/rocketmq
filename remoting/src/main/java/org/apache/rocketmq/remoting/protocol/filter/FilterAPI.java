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
package org.apache.rocketmq.remoting.protocol.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Arrays;

public class FilterAPI {

    /**
     * Build a {@link SubscriptionData} from a topic and a tag expression.
     *
     * <p>The {@code subString} is either
     *    - {@link SubscriptionData#SUB_ALL} ({@code "*"}, meaning "subscribe to everything")
     *    - or a list of tags joined by {@code "||"}.
     * Each tag is recorded twice:
     *    - once verbatim in {@link SubscriptionData#getTagsSet()} (for display/debug)
     *    - and once as its {@code hashCode()} in {@link SubscriptionData#getCodeSet()}
     *      (for the broker-side O(1) tag match against the per-message {@code tagsCode}).
     *
     * <p>Note the hash collision trade-off:
     * because {@code codeSet} stores only 32-bit hashes,
     * a matching code may occasionally be a false positive on the broker,
     * which is why the consumer still performs exact tag filtering after delivery.
     *
     * @param topic     the topic being subscribed
     * @param subString tag expression: {@code "*"} or tags joined by {@code "||"}
     * @return the built subscription data
     * @throws Exception if the tag expression cannot be split
     */
    public static SubscriptionData buildSubscriptionData(String topic, String subString) throws Exception {
        final SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        if (StringUtils.isEmpty(subString) || subString.equals(SubscriptionData.SUB_ALL)) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
            return subscriptionData;
        }
        // split "tagA||tagB" into individual tags; trim and drop empty segments
        String[] tags = subString.split("\\|\\|");
        if (tags.length > 0) {
            Arrays.stream(tags).map(String::trim).filter(tag -> !tag.isEmpty()).forEach(tag -> {
                // store verbatim tag for display plus its hashCode for broker-side matching
                subscriptionData.getTagsSet().add(tag);
                subscriptionData.getCodeSet().add(tag.hashCode());
            });
        } else {
            throw new Exception("subString split error");
        }

        return subscriptionData;
    }

    public static SubscriptionData buildSubscriptionData(String topic, String subString, String expressionType) throws Exception {
        final SubscriptionData subscriptionData = buildSubscriptionData(topic, subString);
        if (StringUtils.isNotBlank(expressionType)) {
            subscriptionData.setExpressionType(expressionType);
        }
        return subscriptionData;
    }

    public static SubscriptionData build(final String topic, final String subString,
        final String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(topic, subString);
        }

        if (StringUtils.isEmpty(subString)) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}
