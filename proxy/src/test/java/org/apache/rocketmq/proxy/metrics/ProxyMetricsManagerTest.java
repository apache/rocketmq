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

package org.apache.rocketmq.proxy.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyMetricsManagerTest {

    @Test
    public void testMetricsConfigValuesContainingColons() throws Exception {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setMetricsExporterType(MetricsExporterType.OTLP_GRPC);
        proxyConfig.setMetricsGrpcExporterTarget("http://127.0.0.1:4317");
        proxyConfig.setMetricsGrpcExporterHeader("authorization:Bearer:token");
        proxyConfig.setMetricsLabel("endpoint:https://collector:4317");

        ProxyMetricsManager metricsManager = ProxyMetricsManager.initClusterMode(proxyConfig);
        metricsManager.start();
        try {
            Attributes attributes = ProxyMetricsManager.newAttributesBuilder().build();
            assertThat(attributes.get(AttributeKey.stringKey("endpoint")))
                .isEqualTo("https://collector:4317");

            Field metricExporterField = ProxyMetricsManager.class.getDeclaredField("metricExporter");
            metricExporterField.setAccessible(true);
            assertThat(metricExporterField.get(metricsManager).toString())
                .contains("authorization=OBFUSCATED");
        } finally {
            metricsManager.shutdown();
        }
    }
}
