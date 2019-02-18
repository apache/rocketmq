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
package org.apache.rocketmq.snode.service;

import io.prometheus.client.CollectorRegistry;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.snode.service.impl.MetricsServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class MetricsServiceImplTest {
    private MetricsServiceImpl metricsService = new MetricsServiceImpl();

    private final String requestCode = "310";

    private final String topic = "Test";

    @Test
    public void testIncRequestCountFalse() throws Exception {
        metricsService.incRequestCount(310, false);
        Double requestFailedTotal = getLabelsValue("request_failed_total", requestCode);
        Double requestTotal = getLabelsValue("request_total", requestCode);
        assertThat(requestFailedTotal).isEqualTo(1.0);
        assertThat(requestTotal);
    }

    @Test
    public void testIncRequestCountTrue() {
        metricsService.incRequestCount(310, true);
        Double requestTotal = getLabelsValue("request_total", requestCode);
        assertThat(requestTotal).isEqualTo(1.0);
    }

    @Test
    public void testRequestSize() throws Exception {
        Field field = MetricsServiceImpl.class.getDeclaredField("statsItemSet");
        field.setAccessible(true);
        metricsService.recordRequestSize(topic, 100);
        StatsItemSet statsItemSet = (StatsItemSet) field.get(metricsService);
        AtomicLong requestSize = statsItemSet.getStatsItem("TotalSize@" + topic).getValue();
        assertThat(requestSize.intValue()).isEqualTo(100);
    }

    public Double getLabelsValue(String name, String labelValue) {
        return CollectorRegistry.defaultRegistry.getSampleValue(name, new String[] {"requestCode"}, new String[] {labelValue});
    }
}
