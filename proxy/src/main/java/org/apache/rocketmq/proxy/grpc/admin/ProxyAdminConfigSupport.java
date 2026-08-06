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

import apache.rocketmq.v2.DescribeProxyConfigRequest;
import apache.rocketmq.v2.DescribeProxyConfigResponse;
import apache.rocketmq.v2.DescribeQuotaRequest;
import apache.rocketmq.v2.DescribeQuotaResponse;
import apache.rocketmq.v2.ProxyRuntimeConfig;
import apache.rocketmq.v2.QuotaDimension;
import apache.rocketmq.v2.QuotaPolicy;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.UpdateProxyConfigRequest;
import apache.rocketmq.v2.UpdateProxyConfigResponse;
import apache.rocketmq.v2.UpdateQuotaRequest;
import apache.rocketmq.v2.UpdateQuotaResponse;
import com.google.protobuf.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;

/**
 * RIP-2 M2 runtime config query & hot update, plus quota visualization &
 * controlled adjustment, served by {@link org.apache.rocketmq.proxy.grpc.admin.ProxyAdminServiceGrpcService}.
 *
 * <p>Hot update semantics: every field carried by {@code ProxyRuntimeConfig} is mapped onto
 * the live {@link ProxyConfig} instance; the response reports the snake_case field names that
 * actually changed. Fields whose effect needs a process restart (ports, TLS material) are
 * still applied to the config object and reported, and additionally listed in the
 * {@code restart_required} documentation of the RIP-2 proposal.
 *
 * <p>proto3 scalar default caveat: an UpdateProxyConfig request cannot express "set this
 * numeric/string field to zero/empty" because default values are indistinguishable from
 * absent fields. Callers should send partial configs containing only the fields to change.
 */
public class ProxyAdminConfigSupport {

    public static final String METRIC_MAX_MESSAGE_SIZE = "MAX_MESSAGE_SIZE";
    public static final String METRIC_GRPC_MAX_CONCURRENT_CALLS = "GRPC_MAX_CONCURRENT_CALLS_PER_CONNECTION";
    public static final String METRIC_GRPC_THREAD_POOL = "GRPC_THREAD_POOL_NUMS";

    /**
     * Quota policies managed by the proxy admin surface, keyed by
     * {@code dimension:resource:metric}. Seeded from the live proxy config on first access;
     * UpdateQuota mutates this registry and the backing ProxyConfig when a direct mapping
     * exists.
     */
    private final ConcurrentMap<String, QuotaPolicy> quotaRegistry = new ConcurrentHashMap<>();
    private volatile boolean seeded;

    // ---------------------------------------------------------------------
    // runtime config
    // ---------------------------------------------------------------------

    public DescribeProxyConfigResponse describeProxyConfig(DescribeProxyConfigRequest request,
        apache.rocketmq.v2.Status ok) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        return DescribeProxyConfigResponse.newBuilder()
            .setStatus(ok)
            .setConfig(toProto(config))
            .build();
    }

    public UpdateProxyConfigResponse updateProxyConfig(UpdateProxyConfigRequest request,
        apache.rocketmq.v2.Status ok) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        ProxyRuntimeConfig incoming = request.getConfig();
        List<String> changedFields = new ArrayList<>();

        if (!incoming.getProxyMode().isEmpty() && !incoming.getProxyMode().equals(config.getProxyMode())) {
            config.setProxyMode(incoming.getProxyMode());
            changedFields.add("proxy_mode");
        }
        if (!incoming.getRocketmqClusterName().isEmpty()
            && !incoming.getRocketmqClusterName().equals(config.getRocketMQClusterName())) {
            config.setRocketMQClusterName(incoming.getRocketmqClusterName());
            changedFields.add("rocketmq_cluster_name");
        }
        if (!incoming.getProxyClusterName().isEmpty()
            && !incoming.getProxyClusterName().equals(config.getProxyClusterName())) {
            config.setProxyClusterName(incoming.getProxyClusterName());
            changedFields.add("proxy_cluster_name");
        }
        if (!incoming.getProxyName().isEmpty() && !incoming.getProxyName().equals(config.getProxyName())) {
            config.setProxyName(incoming.getProxyName());
            changedFields.add("proxy_name");
        }
        if (!incoming.getLocalServeAddr().isEmpty()
            && !incoming.getLocalServeAddr().equals(config.getLocalServeAddr())) {
            config.setLocalServeAddr(incoming.getLocalServeAddr());
            changedFields.add("local_serve_addr");
        }
        if (!incoming.getNamesrvAddr().isEmpty() && !incoming.getNamesrvAddr().equals(config.getNamesrvAddr())) {
            config.setNamesrvAddr(incoming.getNamesrvAddr());
            changedFields.add("namesrv_addr");
        }
        if (incoming.getGrpcServerPort() > 0 && incoming.getGrpcServerPort() != config.getGrpcServerPort()) {
            config.setGrpcServerPort(incoming.getGrpcServerPort());
            changedFields.add("grpc_server_port");
        }
        if (incoming.getGrpcThreadPoolNums() > 0
            && incoming.getGrpcThreadPoolNums() != config.getGrpcThreadPoolNums()) {
            config.setGrpcThreadPoolNums(incoming.getGrpcThreadPoolNums());
            changedFields.add("grpc_thread_pool_nums");
        }
        if (incoming.hasDefaultInvisibleTime()) {
            long millis = toMillis(incoming.getDefaultInvisibleTime());
            if (millis > 0 && millis != config.getDefaultInvisibleTimeMills()) {
                config.setDefaultInvisibleTimeMills(millis);
                changedFields.add("default_invisible_time");
            }
        }
        if (incoming.hasMaxInvisibleTime()) {
            long millis = toMillis(incoming.getMaxInvisibleTime());
            if (millis > 0 && millis != config.getMaxInvisibleTimeMills()) {
                config.setMaxInvisibleTimeMills(millis);
                changedFields.add("max_invisible_time");
            }
        }
        if (incoming.hasMinInvisibleTimeForRecv()) {
            long millis = toMillis(incoming.getMinInvisibleTimeForRecv());
            if (millis > 0 && millis != config.getMinInvisibleTimeMillsForRecv()) {
                config.setMinInvisibleTimeMillsForRecv(millis);
                changedFields.add("min_invisible_time_for_recv");
            }
        }
        if (incoming.hasMaxDelayTime()) {
            long millis = toMillis(incoming.getMaxDelayTime());
            if (millis > 0 && millis != config.getMaxDelayTimeMills()) {
                config.setMaxDelayTimeMills(millis);
                changedFields.add("max_delay_time");
            }
        }
        if (incoming.getMaxMessageSize() > 0 && incoming.getMaxMessageSize() != config.getMaxMessageSize()) {
            config.setMaxMessageSize(incoming.getMaxMessageSize());
            changedFields.add("max_message_size");
        }
        if (incoming.getTlsTestModeEnable() != config.isTlsTestModeEnable()) {
            config.setTlsTestModeEnable(incoming.getTlsTestModeEnable());
            changedFields.add("tls_test_mode_enable");
        }
        if (!incoming.getTlsKeyPath().isEmpty() && !incoming.getTlsKeyPath().equals(config.getTlsKeyPath())) {
            config.setTlsKeyPath(incoming.getTlsKeyPath());
            changedFields.add("tls_key_path");
        }
        if (!incoming.getTlsCertPath().isEmpty() && !incoming.getTlsCertPath().equals(config.getTlsCertPath())) {
            config.setTlsCertPath(incoming.getTlsCertPath());
            changedFields.add("tls_cert_path");
        }
        if (!incoming.getMetricsExporterType().isEmpty()
            && !incoming.getMetricsExporterType().equalsIgnoreCase(String.valueOf(config.getMetricsExporterType()))) {
            try {
                config.setMetricsExporterType(MetricsExporterType.valueOf(
                    StringUtils.upperCase(incoming.getMetricsExporterType())));
                changedFields.add("metrics_exporter_type");
            } catch (IllegalArgumentException ignore) {
                // unknown exporter type: keep current value
            }
        }
        if (incoming.getMetricsPromExporterPort() > 0
            && incoming.getMetricsPromExporterPort() != config.getMetricsPromExporterPort()) {
            config.setMetricsPromExporterPort(incoming.getMetricsPromExporterPort());
            changedFields.add("metrics_prom_exporter_port");
        }
        if (incoming.getTraceOn() != config.isTraceOn()) {
            config.setTraceOn(incoming.getTraceOn());
            changedFields.add("trace_on");
        }
        if (incoming.getProxyAdminEnabled() != config.isProxyAdminEnabled()) {
            config.setProxyAdminEnabled(incoming.getProxyAdminEnabled());
            changedFields.add("proxy_admin_enabled");
        }
        if (incoming.getProxyAdminServerPort() > 0
            && incoming.getProxyAdminServerPort() != safeInt(config.getAdminGrpcPort())) {
            config.setAdminGrpcPort(incoming.getProxyAdminServerPort());
            changedFields.add("proxy_admin_server_port");
        }

        return UpdateProxyConfigResponse.newBuilder()
            .setStatus(ok)
            .setConfig(toProto(ConfigurationManager.getProxyConfig()))
            .addAllChangedFields(changedFields)
            .build();
    }

    public ProxyRuntimeConfig toProto(ProxyConfig config) {
        ProxyRuntimeConfig.Builder builder = ProxyRuntimeConfig.newBuilder()
            .setProxyMode(StringUtils.defaultString(config.getProxyMode()))
            .setRocketmqClusterName(StringUtils.defaultString(config.getRocketMQClusterName()))
            .setProxyClusterName(StringUtils.defaultString(config.getProxyClusterName()))
            .setProxyName(StringUtils.defaultString(config.getProxyName()))
            .setLocalServeAddr(StringUtils.defaultString(config.getLocalServeAddr()))
            .setNamesrvAddr(StringUtils.defaultString(config.getNamesrvAddr()))
            .setGrpcServerPort(safeInt(config.getGrpcServerPort()))
            .setGrpcThreadPoolNums(config.getGrpcThreadPoolNums())
            .setProxyAdminEnabled(config.isProxyAdminEnabled())
            .setProxyAdminServerPort(safeInt(config.getAdminGrpcPort()))
            .setProxyAdminThreadPoolNums(0)
            .setMaxMessageSize(config.getMaxMessageSize())
            .setDefaultInvisibleTime(durationMillis(config.getDefaultInvisibleTimeMills()))
            .setMaxInvisibleTime(durationMillis(config.getMaxInvisibleTimeMills()))
            .setMinInvisibleTimeForRecv(durationMillis(config.getMinInvisibleTimeMillsForRecv()))
            .setMaxDelayTime(durationMillis(config.getMaxDelayTimeMills()))
            .setTlsTestModeEnable(config.isTlsTestModeEnable())
            .setTlsKeyPath(StringUtils.defaultString(config.getTlsKeyPath()))
            .setTlsCertPath(StringUtils.defaultString(config.getTlsCertPath()))
            .setMetricsExporterType(String.valueOf(config.getMetricsExporterType()))
            .setMetricsPromExporterPort(config.getMetricsPromExporterPort())
            .setTraceOn(config.isTraceOn());
        return builder.build();
    }

    // ---------------------------------------------------------------------
    // quota visualization & controlled adjustment
    // ---------------------------------------------------------------------

    public DescribeQuotaResponse describeQuota(DescribeQuotaRequest request, apache.rocketmq.v2.Status ok) {
        seedRegistry();
        DescribeQuotaResponse.Builder builder = DescribeQuotaResponse.newBuilder().setStatus(ok);
        for (QuotaPolicy policy : quotaRegistry.values()) {
            if (request.getDimension() != QuotaDimension.QUOTA_DIMENSION_UNSPECIFIED
                && policy.getDimension() != request.getDimension()) {
                continue;
            }
            if (!request.getResource().getName().isEmpty()
                && !request.getResource().getName().equals(policy.getResource().getName())) {
                continue;
            }
            builder.addPolicies(policy);
        }
        return builder.build();
    }

    public UpdateQuotaResponse updateQuota(UpdateQuotaRequest request, apache.rocketmq.v2.Status ok,
        apache.rocketmq.v2.Status badRequest) {
        seedRegistry();
        QuotaPolicy policy = request.getPolicy();
        if (policy == null || policy.getLimit() <= 0 || StringUtils.isBlank(policy.getMetric())) {
            return UpdateQuotaResponse.newBuilder()
                .setStatus(badRequest)
                .build();
        }
        String key = quotaKey(policy.getDimension(), policy.getResource().getName(), policy.getMetric());
        QuotaPolicy.Builder updated = policy.toBuilder();
        // Apply to the live proxy config when the metric maps onto a real proxy control knob,
        // so the adjustment takes effect immediately instead of being only bookkeeping.
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        if (METRIC_MAX_MESSAGE_SIZE.equals(policy.getMetric())) {
            config.setMaxMessageSize((int) policy.getLimit());
        } else if (METRIC_GRPC_MAX_CONCURRENT_CALLS.equals(policy.getMetric())) {
            config.setGrpcMaxConcurrentCallsPerConnection((int) policy.getLimit());
        } else if (METRIC_GRPC_THREAD_POOL.equals(policy.getMetric())) {
            config.setGrpcThreadPoolNums((int) policy.getLimit());
        }
        if (!updated.hasWindow()) {
            updated.setWindow(Duration.newBuilder().setSeconds(60).build());
        }
        quotaRegistry.put(key, updated.build());
        return UpdateQuotaResponse.newBuilder()
            .setStatus(ok)
            .setPolicy(updated.build())
            .build();
    }

    private void seedRegistry() {
        if (seeded) {
            return;
        }
        synchronized (this) {
            if (seeded) {
                return;
            }
            ProxyConfig config = ConfigurationManager.getProxyConfig();
            Duration window = Duration.newBuilder().setSeconds(60).build();
            putSeed(QuotaDimension.QUOTA_DIMENSION_TOPIC, "*", METRIC_MAX_MESSAGE_SIZE, config.getMaxMessageSize(), window);
            putSeed(QuotaDimension.QUOTA_DIMENSION_GROUP, "*", METRIC_GRPC_MAX_CONCURRENT_CALLS,
                config.getGrpcMaxConcurrentCallsPerConnection(), window);
            putSeed(QuotaDimension.QUOTA_DIMENSION_GROUP, "*", METRIC_GRPC_THREAD_POOL, config.getGrpcThreadPoolNums(), window);
            seeded = true;
        }
    }

    private void putSeed(QuotaDimension dimension, String resourceName, String metric, long limit, Duration window) {
        QuotaPolicy policy = QuotaPolicy.newBuilder()
            .setDimension(dimension)
            .setResource(Resource.newBuilder().setName(resourceName).build())
            .setMetric(metric)
            .setLimit(limit)
            .setCurrentUsage(0)
            .setRecentTriggerCount(0)
            .setWindow(window)
            .build();
        quotaRegistry.put(quotaKey(dimension, resourceName, metric), policy);
    }

    private static String quotaKey(QuotaDimension dimension, String resourceName, String metric) {
        return dimension.name() + ":" + StringUtils.defaultString(resourceName) + ":" + metric;
    }

    private static long toMillis(Duration duration) {
        return duration.getSeconds() * 1000L + duration.getNanos() / 1_000_000L;
    }

    private static Duration durationMillis(long millis) {
        return Duration.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }

    private static int safeInt(Integer value) {
        return value == null ? 0 : value;
    }

    Map<String, QuotaPolicy> getQuotaRegistryView() {
        seedRegistry();
        return new java.util.HashMap<>(quotaRegistry);
    }

    List<QuotaPolicy> allQuotaPolicies() {
        seedRegistry();
        return new ArrayList<>(quotaRegistry.values());
    }
}
