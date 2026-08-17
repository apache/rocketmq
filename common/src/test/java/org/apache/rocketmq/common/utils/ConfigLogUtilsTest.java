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

import java.util.Properties;
import org.apache.rocketmq.common.annotation.SensitiveConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfigLogUtilsTest {

    private static class AnnotatedConfig {
        @SensitiveConfig
        private String opaqueValue = "top-secret";
    }

    private static class GetterAnnotatedConfig {
        @SensitiveConfig
        public String getInheritedValue() {
            return "top-secret";
        }
    }

    @Test
    public void testRedactSensitivePropertiesWithoutChangingSource() {
        Properties source = new Properties();
        source.setProperty("opaqueValue", "top-secret");
        source.setProperty("databasePassword", "not-annotated");

        Properties redacted = ConfigLogUtils.redactSensitiveProperties(source,
            ConfigLogUtils.getSensitiveConfigProperties(new AnnotatedConfig()));

        assertThat(redacted.getProperty("opaqueValue")).isEqualTo("to******et");
        assertThat(redacted.getProperty("databasePassword")).isEqualTo("not-annotated");
        assertThat(source.getProperty("opaqueValue")).isEqualTo("top-secret");
    }

    @Test
    public void testSensitiveConfigAnnotationMasksUnremarkablePropertyName() {
        AnnotatedConfig config = new AnnotatedConfig();

        assertThat(ConfigLogUtils.getValueForLog(config, "opaqueValue", config.opaqueValue))
            .isEqualTo("to******et");
    }

    @Test
    public void testSensitiveConfigAnnotationOnGetterMasksProperty() {
        GetterAnnotatedConfig config = new GetterAnnotatedConfig();

        assertThat(ConfigLogUtils.getValueForLog(config, "inheritedValue", config.getInheritedValue()))
            .isEqualTo("to******et");
    }

    @Test
    public void testMaskSensitiveValueKeepsSafePrefixAndSuffix() {
        assertThat(ConfigLogUtils.maskSensitiveValue("1234")).isEqualTo("******");
        assertThat(ConfigLogUtils.maskSensitiveValue("12345")).isEqualTo("1******5");
        assertThat(ConfigLogUtils.maskSensitiveValue("12345678")).isEqualTo("12******78");
        assertThat(ConfigLogUtils.maskSensitiveValue("")).isEqualTo("");
    }
}
