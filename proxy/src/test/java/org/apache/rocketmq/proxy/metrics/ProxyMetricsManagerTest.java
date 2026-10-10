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

import java.util.Map;
import org.junit.Test;

import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.assertThat;

public class ProxyMetricsManagerTest {

    @Test
    public void parseKeyValuePairsShouldPreserveColonsInValues() {
        Map<String, String> result = ProxyMetricsManager.parseKeyValuePairs(
            "metricsGrpcExporterHeader", "Authorization:Bearer token:part,endpoint:https://collector:4317");

        assertThat(result)
            .containsEntry("Authorization", "Bearer token:part")
            .containsEntry("endpoint", "https://collector:4317");
    }

    @Test
    public void parseKeyValuePairsShouldTrimEntriesAndAllowEmptyValues() {
        Map<String, String> result = ProxyMetricsManager.parseKeyValuePairs(
            "metricsLabel", " cluster : production ,optional:");

        assertThat(result)
            .containsEntry("cluster", "production")
            .containsEntry("optional", "");
    }

    @Test
    public void parseKeyValuePairsShouldSkipMalformedEntriesAndEmptyKeys() {
        Map<String, String> result = ProxyMetricsManager.parseKeyValuePairs(
            "metricsGrpcExporterHeader", "missing-separator, :secret,valid:value");

        assertThat(result).containsExactly(entry("valid", "value"));
    }
}
