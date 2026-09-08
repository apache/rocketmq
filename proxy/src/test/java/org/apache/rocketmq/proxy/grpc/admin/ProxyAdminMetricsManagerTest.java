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
package org.apache.rocketmq.proxy.grpc.admin;

import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProxyAdminMetricsManagerTest {

    @After
    public void tearDown() {
        // reset the static pipeline between tests; shutdown() is idempotent
        ProxyAdminMetricsManager.shutdown();
    }

    private static ProxyConfig configWith(MetricsExporterType exporterType) {
        ProxyConfig config = new ProxyConfig();
        config.setMetricsExporterType(exporterType);
        return config;
    }

    @Test
    public void disabledExporterStaysUninitializedAndRecordsAreNoOps() {
        ProxyAdminMetricsManager.init(configWith(MetricsExporterType.DISABLE));
        assertFalse(ProxyAdminMetricsManager.isInitialized());
        // record methods must be safe no-ops while uninitialized
        ProxyAdminMetricsManager.recordSuccess("ListClients", 1L);
        ProxyAdminMetricsManager.recordError("ListClients", 1L, new RuntimeException("x"));
        assertFalse(ProxyAdminMetricsManager.isInitialized());
    }

    @Test
    public void otlpExporterWithoutTargetStaysUninitialized() {
        ProxyConfig config = configWith(MetricsExporterType.OTLP_GRPC);
        // default metricsGrpcExporterTarget is blank -> warning branch, not initialized
        ProxyAdminMetricsManager.init(config);
        assertFalse(ProxyAdminMetricsManager.isInitialized());
    }

    @Test
    public void otlpExporterInitializesRecordsAndShutsDown() {
        ProxyConfig config = configWith(MetricsExporterType.OTLP_GRPC);
        config.setMetricsGrpcExporterTarget("127.0.0.1:4317");
        config.setMetricsInDelta(true);
        config.setMetricGrpcExporterTimeOutInMills(100L);
        config.setMetricGrpcExporterIntervalInMills(60_000L);
        config.setMetricsGrpcExporterHeader("k1:v1,bad-no-colon,k2:v2,,");
        ProxyAdminMetricsManager.init(config);
        assertTrue(ProxyAdminMetricsManager.isInitialized());
        ProxyAdminMetricsManager.recordSuccess("ListClients", 5L);
        ProxyAdminMetricsManager.recordError("QueryMessage", 7L, new IllegalArgumentException("boom"));
        // null error falls back to the "unknown" error_type label
        ProxyAdminMetricsManager.recordError("QueryMessage", 7L, null);
        ProxyAdminMetricsManager.shutdown();
        assertFalse(ProxyAdminMetricsManager.isInitialized());

        // re-init with CUMULATIVE temporality covers the other selector branch
        config.setMetricsInDelta(false);
        ProxyAdminMetricsManager.init(config);
        assertTrue(ProxyAdminMetricsManager.isInitialized());
        ProxyAdminMetricsManager.recordSuccess("GetTopicRoute", 2L);
        ProxyAdminMetricsManager.shutdown();
        assertFalse(ProxyAdminMetricsManager.isInitialized());
    }

    @Test
    public void logExporterInitializesAndShutsDown() {
        ProxyConfig config = configWith(MetricsExporterType.LOG);
        config.setMetricsInDelta(false);
        config.setMetricLoggingExporterIntervalInMills(60_000L);
        ProxyAdminMetricsManager.init(config);
        assertTrue(ProxyAdminMetricsManager.isInitialized());
        ProxyAdminMetricsManager.recordSuccess("DescribeSubscription", 3L);
        ProxyAdminMetricsManager.recordError("ResetGroupOffset", 4L, new IllegalStateException("bad"));
        ProxyAdminMetricsManager.shutdown();
        assertFalse(ProxyAdminMetricsManager.isInitialized());
    }

    @Test
    public void promExporterBindsServerAndShutsDown() {
        ProxyConfig config = configWith(MetricsExporterType.PROM);
        // blank host falls back to 0.0.0.0
        config.setMetricsPromExporterHost("");
        config.setMetricsPromExporterPort(35557);
        ProxyAdminMetricsManager.init(config);
        assertTrue(ProxyAdminMetricsManager.isInitialized());
        ProxyAdminMetricsManager.recordSuccess("AdminSendMessage", 6L);
        ProxyAdminMetricsManager.shutdown();
        assertFalse(ProxyAdminMetricsManager.isInitialized());
    }
}
