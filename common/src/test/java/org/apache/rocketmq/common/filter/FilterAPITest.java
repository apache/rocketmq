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

package org.apache.rocketmq.common.filter;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterAPITest {
    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private String subString = "TAG1 || Tag2 || tag3";

    @Test
    public void testBuildSubscriptionData() throws Exception {
        SubscriptionData subscriptionData =
            FilterAPI.buildSubscriptionData(group, topic, subString);
        assertThat(subscriptionData.getTopic()).isEqualTo(topic);
        assertThat(subscriptionData.getSubString()).isEqualTo(subString);
        String [] tags = subString.split("\\|\\|");
        Set<String> tagSet = new HashSet<>();
        for (String tag : tags) {
            tagSet.add(tag.trim());
        }
        assertThat(subscriptionData.getTagsSet()).isEqualTo(tagSet);
    }
}
