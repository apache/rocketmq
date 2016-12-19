/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.common.filter;

import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;


/**
 * @author shijia.wxr
 *
 */
public class FilterAPITest {

    @Test
    public void testBuildSubscriptionData() throws Exception {
        SubscriptionData subscriptionData =
                FilterAPI.buildSubscriptionData("ConsumerGroup1", "TestTopic", "TAG1 || Tag2 || tag3");
        System.out.println(subscriptionData);
    }

    @Test
    public void testSubscriptionData() throws Exception {
        SubscriptionData subscriptionData =
                FilterAPI.buildSubscriptionData("ConsumerGroup1", "TestTopic", "TAG1 || Tag2 || tag3");
        subscriptionData.setFilterClassSource("java hello");
        String json = RemotingSerializable.toJson(subscriptionData, true);
        System.out.println(json);
    }
}
