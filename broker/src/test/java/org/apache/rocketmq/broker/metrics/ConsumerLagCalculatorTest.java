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
package org.apache.rocketmq.broker.metrics;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.common.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerLagCalculatorTest {

    private ConsumerLagCalculator consumerLagCalculator;

    @Mock
    private BrokerController brokerController;

    @Test
    public void testUnknownTopicHasNoLagAndNoLatency() throws Exception {
        // the constructor walks the pop processor as well
        when(brokerController.getPopMessageProcessor()).thenReturn(mock(PopMessageProcessor.class));
        consumerLagCalculator = new ConsumerLagCalculator(brokerController);

        // a POP group whose retry topic does not exist yet is queried with a null topic
        Pair<Long, Long> lag = consumerLagCalculator.getConsumerLagStats("group", null, true);
        assertEquals(0L, lag.getObject1().longValue());
        // the "no timestamp" sentinel must be normalized here like the regular path does, otherwise
        // the recorded latency becomes 'now - Long.MAX_VALUE' (about -9.2e18 ms)
        assertEquals(0L, lag.getObject2().longValue());

        ConsumerLagCalculator.CalculateLagResult result =
            new ConsumerLagCalculator.CalculateLagResult("group", "topic", true);
        result.earliestUnconsumedTimestamp = lag.getObject2();
        assertEquals(0L, result.getLagLatency());

        Pair<Long, Long> inFlight = consumerLagCalculator.getInFlightMsgStats("group", null, true);
        assertEquals(0L, inFlight.getObject1().longValue());
        assertEquals(0L, inFlight.getObject2().longValue());
    }
}
