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

package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.annotation.Sensitive;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfigLogUtilsTest {

    private static class AnnotatedConfig {
        @Sensitive
        private String opaqueValue = "top-secret";
    }

    @Test
    public void testSensitiveAnnotationMasksUnremarkablePropertyName() {
        AnnotatedConfig config = new AnnotatedConfig();

        assertThat(ConfigLogUtils.getValueForLog(config, "opaqueValue", config.opaqueValue))
            .isEqualTo("******");
    }

    @Test
    public void testControllerMetricsHeaderIsMaskedWithoutChangingConfiguration() {
        ControllerConfig config = new ControllerConfig();
        config.setMetricsGrpcExporterHeader("Authorization:secret-token");

        assertThat(ConfigLogUtils.getValueForLog(config, "metricsGrpcExporterHeader",
            config.getMetricsGrpcExporterHeader())).isEqualTo("******");
        assertThat(config.getMetricsGrpcExporterHeader()).isEqualTo("Authorization:secret-token");
    }

    @Test
    public void testMaskSensitiveValueHidesEntireValue() {
        assertThat(ConfigLogUtils.maskSensitiveValue("1")).isEqualTo("******");
        assertThat(ConfigLogUtils.maskSensitiveValue("1234")).isEqualTo("******");
        assertThat(ConfigLogUtils.maskSensitiveValue("12345")).isEqualTo("******");
        assertThat(ConfigLogUtils.maskSensitiveValue("12345678")).isEqualTo("******");
        assertThat(ConfigLogUtils.maskSensitiveValue("{\"secretKey\":\"top-secret\"}"))
            .isEqualTo("******");
        assertThat(ConfigLogUtils.maskSensitiveValue("")).isEqualTo("");
        assertThat(ConfigLogUtils.maskSensitiveValue(null)).isNull();
    }
}
