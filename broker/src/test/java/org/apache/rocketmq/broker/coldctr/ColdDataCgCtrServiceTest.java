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

package org.apache.rocketmq.broker.coldctr;

import java.lang.reflect.Method;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.coldctr.AccAndTimeStamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColdDataCgCtrServiceTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private BrokerConfig brokerConfig;

    private ColdDataCgCtrService coldDataCgCtrService;

    @Before
    public void init() throws IllegalAccessException {
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        coldDataCgCtrService = new ColdDataCgCtrService(brokerController);
        FieldUtils.writeField(coldDataCgCtrService, "cgColdThresholdMapRuntime", createCgColdThresholdMapRuntime(), true);
        FieldUtils.writeField(coldDataCgCtrService, "cgColdThresholdMapConfig", createCgColdThresholdMapConfig(), true);
    }

    @Test
    public void testGetColdDataFlowCtrInfo() {
        String actual = coldDataCgCtrService.getColdDataFlowCtrInfo();
        assertTrue(actual.contains("\"globalAcc\":0"));
        assertTrue(actual.contains("\"cgColdReadThreshold\":0"));
        assertTrue(actual.contains("\"globalColdReadThreshold\":0"));
        assertTrue(actual.contains("\"configTable\":{\"consumerGroup2\":2048}"));
        assertTrue(actual.contains("\"runtimeTable\":{\"consumerGroup1\":{\"coldAcc\":1,\"createTimeMills\":1,\"lastColdReadTimeMills\":1}}"));
    }

    @Test
    public void testDeceleratesHighestThresholdsWithoutOverflow() throws Exception {
        ConcurrentHashMap<String, Long> thresholds = new ConcurrentHashMap<>();
        thresholds.put("highest||adaptive", Long.MAX_VALUE);
        thresholds.put("medium2||adaptive", 2L);
        thresholds.put("medium1||adaptive", 1L);
        thresholds.put("lowest||adaptive", 0L);
        ColdCtrStrategy strategy = mock(ColdCtrStrategy.class);
        FieldUtils.writeField(coldDataCgCtrService, "cgColdThresholdMapConfig", thresholds, true);
        FieldUtils.writeField(coldDataCgCtrService, "coldCtrStrategy", strategy, true);
        Method method = ColdDataCgCtrService.class.getDeclaredMethod("sortAndDecelerate");
        method.setAccessible(true);

        method.invoke(coldDataCgCtrService);

        verify(strategy).decelerate(eq("highest||adaptive"), anyLong());
        verify(strategy, never()).decelerate(eq("lowest||adaptive"), anyLong());
    }

    private Map<String, AccAndTimeStamp> createCgColdThresholdMapRuntime() {
        Map<String, AccAndTimeStamp> result = new ConcurrentHashMap<>();
        AccAndTimeStamp accAndTimeStamp = new AccAndTimeStamp(new AtomicLong(1L));
        accAndTimeStamp.setCreateTimeMills(1L);
        accAndTimeStamp.setLastColdReadTimeMills(1L);
        result.put("consumerGroup1", accAndTimeStamp);
        return result;
    }

    private ConcurrentHashMap<String, Long> createCgColdThresholdMapConfig() {
        ConcurrentHashMap<String, Long> result = new ConcurrentHashMap<>();
        result.put("consumerGroup2", 2048L);
        return result;
    }
}
