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
package org.apache.rocketmq.remoting.protocol.admin;

import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ConsumeStatsTest {

    @Test
    public void testComputeTotalDiff() {
        ConsumeStats stats = new ConsumeStats();
        MessageQueue messageQueue = Mockito.mock(MessageQueue.class);
        OffsetWrapper offsetWrapper = Mockito.mock(OffsetWrapper.class);
        Mockito.when(offsetWrapper.getConsumerOffset()).thenReturn(1L);
        Mockito.when(offsetWrapper.getBrokerOffset()).thenReturn(2L);
        stats.getOffsetTable().put(messageQueue, offsetWrapper);

        MessageQueue messageQueue2 = Mockito.mock(MessageQueue.class);
        OffsetWrapper offsetWrapper2 = Mockito.mock(OffsetWrapper.class);
        Mockito.when(offsetWrapper2.getConsumerOffset()).thenReturn(2L);
        Mockito.when(offsetWrapper2.getBrokerOffset()).thenReturn(3L);
        stats.getOffsetTable().put(messageQueue2, offsetWrapper2);
        Assert.assertEquals(2L, stats.computeTotalDiff());
    }

    @Test
    public void testComputeInflightTotalDiff() {
        ConsumeStats stats = new ConsumeStats();
        MessageQueue messageQueue = Mockito.mock(MessageQueue.class);
        OffsetWrapper offsetWrapper = Mockito.mock(OffsetWrapper.class);
        Mockito.when(offsetWrapper.getBrokerOffset()).thenReturn(3L);
        Mockito.when(offsetWrapper.getPullOffset()).thenReturn(2L);
        stats.getOffsetTable().put(messageQueue, offsetWrapper);

        MessageQueue messageQueue2 = Mockito.mock(MessageQueue.class);
        OffsetWrapper offsetWrapper2 = Mockito.mock(OffsetWrapper.class);
        Mockito.when(offsetWrapper.getBrokerOffset()).thenReturn(3L);
        Mockito.when(offsetWrapper.getPullOffset()).thenReturn(2L);
        stats.getOffsetTable().put(messageQueue2, offsetWrapper2);
        Assert.assertEquals(2L, stats.computeInflightTotalDiff());
    }
}
