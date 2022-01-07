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

package org.apache.rocketmq.broker;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BrokerPathConfigHelperTest {

    @Test
    public void testGetLmqConsumerOffsetPath() {
        String lmqConsumerOffsetPath = BrokerPathConfigHelper.getLmqConsumerOffsetPath("/home/admin/store");
        assertEquals("/home/admin/store/config/lmqConsumerOffset.json", lmqConsumerOffsetPath);


        String consumerOffsetPath = BrokerPathConfigHelper.getConsumerOffsetPath("/home/admin/store");
        assertEquals("/home/admin/store/config/consumerOffset.json", consumerOffsetPath);

        String topicConfigPath = BrokerPathConfigHelper.getTopicConfigPath("/home/admin/store");
        assertEquals("/home/admin/store/config/topics.json", topicConfigPath);

        String subscriptionGroupPath = BrokerPathConfigHelper.getSubscriptionGroupPath("/home/admin/store");
        assertEquals("/home/admin/store/config/subscriptionGroup.json", subscriptionGroupPath);

    }
}