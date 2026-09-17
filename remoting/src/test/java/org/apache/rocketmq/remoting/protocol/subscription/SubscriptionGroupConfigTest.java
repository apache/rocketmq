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
package org.apache.rocketmq.remoting.protocol.subscription;

import org.junit.Assert;
import org.junit.Test;

public class SubscriptionGroupConfigTest {

    @Test
    public void testBrokerIdParticipatesInEqualsAndHashCode() {
        SubscriptionGroupConfig first = new SubscriptionGroupConfig();
        first.setGroupName("groupA");
        first.setBrokerId(0L);

        SubscriptionGroupConfig second = new SubscriptionGroupConfig();
        second.setGroupName("groupA");
        second.setBrokerId(0L);

        Assert.assertEquals(first, second);
        Assert.assertEquals(first.hashCode(), second.hashCode());

        // brokerId is part of hashCode, so it must be part of equals as well:
        // otherwise two equal configs could carry different hash codes
        second.setBrokerId(1L);
        Assert.assertNotEquals(first, second);
        Assert.assertNotEquals(first.hashCode(), second.hashCode());
    }
}
