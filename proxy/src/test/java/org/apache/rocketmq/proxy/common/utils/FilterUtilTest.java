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

package org.apache.rocketmq.proxy.common.utils;

import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterUtilTest {
    @Test
    public void testTagMatched() throws Exception {
        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData("", "tagA");
        assertThat(FilterUtils.isTagMatched(subscriptionData.getTagsSet(), "tagA")).isTrue();
    }

    @Test
    public void testTagNotMatched() throws Exception {
        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData("", "tagA");
        assertThat(FilterUtils.isTagMatched(subscriptionData.getTagsSet(), "tagB")).isFalse();
    }

    @Test
    public void testTagMatchedStar() throws Exception {
        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData("", "*");
        assertThat(FilterUtils.isTagMatched(subscriptionData.getTagsSet(), "tagA")).isTrue();
    }

    @Test
    public void testTagNotMatchedNull() throws Exception {
        SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData("", "tagA");
        assertThat(FilterUtils.isTagMatched(subscriptionData.getTagsSet(), null)).isFalse();
    }

    @Test
    public void testBuildSubscriptionData() throws Exception {
        // Test case 1: expressionType is null, will use TAG as default.
        String topic = "topic";
        String subString = "substring";
        String expressionType = null;
        SubscriptionData result = FilterAPI.buildSubscriptionData(topic, subString, expressionType);
        assertThat(result).isNotNull();
        assertThat(topic).isEqualTo(result.getTopic());
        assertThat(subString).isEqualTo(result.getSubString());
        assertThat(result.getExpressionType()).isEqualTo("TAG");
        assertThat(result.getCodeSet().size()).isEqualTo(1);

        // Test case 2: expressionType is not null
        topic = "topic";
        subString = "substring1||substring2";
        expressionType = "SQL92";
        result = FilterAPI.buildSubscriptionData(topic, subString, expressionType);
        assertThat(result).isNotNull();
        assertThat(topic).isEqualTo(result.getTopic());
        assertThat(subString).isEqualTo(result.getSubString());
        assertThat(result.getExpressionType()).isEqualTo(expressionType);
        assertThat(result.getCodeSet().size()).isEqualTo(2);
    }

}
