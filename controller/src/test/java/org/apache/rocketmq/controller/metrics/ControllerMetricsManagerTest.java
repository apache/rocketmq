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

package org.apache.rocketmq.controller.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.controller.ControllerManager;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ControllerMetricsManagerTest {

    @Test
    public void testMetricsConfigValuesContainingColons() throws Exception {
        ControllerConfig controllerConfig = new ControllerConfig();
        controllerConfig.setControllerDLegerGroup("group");
        controllerConfig.setControllerDLegerSelfId("n0");
        controllerConfig.setControllerDLegerPeers("n0-127.0.0.1:9878");
        controllerConfig.setMetricsExporterType(MetricsExporterType.OTLP_GRPC);
        controllerConfig.setMetricsGrpcExporterTarget("http://127.0.0.1:4317");
        controllerConfig.setMetricsGrpcExporterHeader("authorization:Bearer:token");
        controllerConfig.setMetricsLabel("endpoint:https://collector:4317");

        ControllerManager controllerManager = mock(ControllerManager.class);
        when(controllerManager.getControllerConfig()).thenReturn(controllerConfig);

        ControllerMetricsManager metricsManager = ControllerMetricsManager.getInstance(controllerManager);
        Attributes attributes = ControllerMetricsManager.newAttributesBuilder().build();

        assertThat(attributes.get(AttributeKey.stringKey("endpoint")))
            .isEqualTo("https://collector:4317");

        Field metricExporterField = ControllerMetricsManager.class.getDeclaredField("metricExporter");
        metricExporterField.setAccessible(true);
        assertThat(metricExporterField.get(metricsManager).toString())
            .contains("authorization=OBFUSCATED");
    }
}
