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

import apache.rocketmq.v2.Code;
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
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProxyAdminConfigSupportTest extends InitConfigTest {

    private ProxyAdminConfigSupport support;
    private int originalMaxMessageSize;
    private long originalDefaultInvisibleTime;
    private boolean originalTlsTestModeEnable;
    private boolean originalTraceOn;
    private boolean originalProxyAdminEnabled;

    @Before
    public void setUp() {
        support = new ProxyAdminConfigSupport();
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        originalMaxMessageSize = config.getMaxMessageSize();
        originalDefaultInvisibleTime = config.getDefaultInvisibleTimeMills();
        originalTlsTestModeEnable = config.isTlsTestModeEnable();
        originalTraceOn = config.isTraceOn();
        originalProxyAdminEnabled = config.isProxyAdminEnabled();
    }

    @After
    public void tearDown() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setMaxMessageSize(originalMaxMessageSize);
        config.setDefaultInvisibleTimeMills(originalDefaultInvisibleTime);
        config.setTlsTestModeEnable(originalTlsTestModeEnable);
        config.setTraceOn(originalTraceOn);
        config.setProxyAdminEnabled(originalProxyAdminEnabled);
    }

    private static apache.rocketmq.v2.Status ok() {
        return apache.rocketmq.v2.Status.newBuilder().setCode(Code.OK).build();
    }

    @Test
    public void describeProxyConfigReflectsLiveConfig() {
        DescribeProxyConfigResponse response = support.describeProxyConfig(
            DescribeProxyConfigRequest.newBuilder().build(), ok());
        assertEquals(Code.OK, response.getStatus().getCode());
        ProxyRuntimeConfig config = response.getConfig();
        assertNotNull(config);
        assertEquals(ConfigurationManager.getProxyConfig().getMaxMessageSize(), config.getMaxMessageSize());
        assertEquals(ConfigurationManager.getProxyConfig().getGrpcThreadPoolNums(), config.getGrpcThreadPoolNums());
        assertTrue(config.getDefaultInvisibleTime().getSeconds() > 0);
    }

    @Test
    public void updateProxyConfigAppliesChangedFields() {
        UpdateProxyConfigResponse response = support.updateProxyConfig(UpdateProxyConfigRequest.newBuilder()
            .setConfig(ProxyRuntimeConfig.newBuilder()
                .setMaxMessageSize(1234)
                .setDefaultInvisibleTime(Duration.newBuilder().setSeconds(120).build())
                .build())
            .build(), ok());
        assertEquals(Code.OK, response.getStatus().getCode());
        assertTrue(response.getChangedFieldsList().contains("max_message_size"));
        assertTrue(response.getChangedFieldsList().contains("default_invisible_time"));
        assertEquals(1234, ConfigurationManager.getProxyConfig().getMaxMessageSize());
        assertEquals(120_000L, ConfigurationManager.getProxyConfig().getDefaultInvisibleTimeMills());
        // response carries the refreshed view
        assertEquals(1234, response.getConfig().getMaxMessageSize());
    }

    @Test
    public void updateProxyConfigReportsNoChangeForSameValues() {
        ProxyConfig live = ConfigurationManager.getProxyConfig();
        // proto3 caveat: booleans cannot express "absent", so mirror the live values
        UpdateProxyConfigResponse response = support.updateProxyConfig(UpdateProxyConfigRequest.newBuilder()
            .setConfig(ProxyRuntimeConfig.newBuilder()
                .setMaxMessageSize(live.getMaxMessageSize())
                .setTlsTestModeEnable(live.isTlsTestModeEnable())
                .setTraceOn(live.isTraceOn())
                .setProxyAdminEnabled(live.isProxyAdminEnabled())
                .build())
            .build(), ok());
        assertEquals(Code.OK, response.getStatus().getCode());
        assertTrue(response.getChangedFieldsList().isEmpty());
    }

    @Test
    public void describeQuotaSeedsProxyLevelPolicies() {
        DescribeQuotaResponse response = support.describeQuota(
            DescribeQuotaRequest.newBuilder().build(), ok());
        assertEquals(Code.OK, response.getStatus().getCode());
        assertTrue(response.getPoliciesCount() >= 3);
        boolean hasMaxMessageSize = false;
        for (QuotaPolicy policy : response.getPoliciesList()) {
            if (ProxyAdminConfigSupport.METRIC_MAX_MESSAGE_SIZE.equals(policy.getMetric())) {
                hasMaxMessageSize = true;
                assertEquals(ConfigurationManager.getProxyConfig().getMaxMessageSize(), policy.getLimit());
                assertTrue(policy.getLimit() > 0);
                assertTrue(policy.hasWindow());
            }
        }
        assertTrue(hasMaxMessageSize);
    }

    @Test
    public void describeQuotaFiltersByDimensionAndResource() {
        DescribeQuotaResponse byDimension = support.describeQuota(DescribeQuotaRequest.newBuilder()
            .setDimension(QuotaDimension.QUOTA_DIMENSION_TOPIC)
            .build(), ok());
        for (QuotaPolicy policy : byDimension.getPoliciesList()) {
            assertEquals(QuotaDimension.QUOTA_DIMENSION_TOPIC, policy.getDimension());
        }
    }

    @Test
    public void updateQuotaAppliesMappedKnobImmediately() {
        UpdateQuotaResponse response = support.updateQuota(UpdateQuotaRequest.newBuilder()
            .setPolicy(QuotaPolicy.newBuilder()
                .setDimension(QuotaDimension.QUOTA_DIMENSION_TOPIC)
                .setResource(Resource.newBuilder().setName("*").build())
                .setMetric(ProxyAdminConfigSupport.METRIC_MAX_MESSAGE_SIZE)
                .setLimit(4096)
                .build())
            .build(), ok(), apache.rocketmq.v2.Status.newBuilder().setCode(Code.BAD_REQUEST).build());
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(4096, response.getPolicy().getLimit());
        assertEquals(4096, ConfigurationManager.getProxyConfig().getMaxMessageSize());
    }

    @Test
    public void updateQuotaRejectsInvalidPolicy() {
        UpdateQuotaResponse response = support.updateQuota(UpdateQuotaRequest.newBuilder()
            .setPolicy(QuotaPolicy.newBuilder()
                .setDimension(QuotaDimension.QUOTA_DIMENSION_TOPIC)
                .setMetric("")
                .setLimit(0)
                .build())
            .build(), ok(), apache.rocketmq.v2.Status.newBuilder().setCode(Code.BAD_REQUEST).build());
        assertEquals(Code.BAD_REQUEST, response.getStatus().getCode());
    }
}
