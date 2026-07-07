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
package org.apache.rocketmq.controller.elect.impl;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultElectPolicyTest {

    @Test
    public void testElectPrefersHigherOffsetWhenOffsetDifferenceExceedsIntegerMaxValue() {
        long lowerOffset = 0L;
        long higherOffset = (long) Integer.MAX_VALUE + 1L;
        Set<Long> brokers = new HashSet<>();
        brokers.add(1L);
        brokers.add(2L);

        DefaultElectPolicy electPolicy = new DefaultElectPolicy((clusterName, brokerName, brokerId) -> true,
            (clusterName, brokerName, brokerId) -> {
                if (brokerId == 1L) {
                    return new BrokerLiveInfo(brokerName, "127.0.0.1:10911", brokerId,
                        System.currentTimeMillis(), 3000L, null, 1, lowerOffset, 0);
                }
                return new BrokerLiveInfo(brokerName, "127.0.0.1:10912", brokerId,
                    System.currentTimeMillis(), 3000L, null, 1, higherOffset, 0);
            });

        Long electedBrokerId = electPolicy.elect("cluster", "broker", brokers, null, null, null);

        assertEquals(Long.valueOf(2L), electedBrokerId);
    }
}
