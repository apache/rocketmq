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
package org.apache.rocketmq.auth.config;

import com.alibaba.fastjson2.JSON;
import java.util.Properties;
import org.apache.rocketmq.common.MixAll;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AuthConfigTest {

    @Test
    public void authenticationRequiredTracksEnabledStateAndWhitelistUpdates() {
        AuthConfig authConfig = new AuthConfig();

        assertThat(authConfig.isAuthenticationRequired("10")).isFalse();

        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationWhitelist(" 10, 11,10 ");
        assertThat(authConfig.isAuthenticationRequired("10")).isFalse();
        assertThat(authConfig.isAuthenticationRequired("11")).isFalse();
        assertThat(authConfig.isAuthenticationRequired("12")).isTrue();
        assertThat(authConfig.isAuthenticationRequired("")).isTrue();
        assertThat(authConfig.isAuthenticationRequired(null)).isTrue();

        authConfig.setAuthenticationWhitelist("12");
        assertThat(authConfig.isAuthenticationRequired("10")).isTrue();
        assertThat(authConfig.isAuthenticationRequired("12")).isFalse();
    }

    @Test
    public void authenticationWhitelistPreservesConfigBindingAndCloneIsolation() {
        String whitelist = " AUTH_A,AUTH_B,AUTH_A ";
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationWhitelist(whitelist);

        Properties properties = MixAll.object2Properties(authConfig);
        assertThat(properties)
            .containsEntry("authenticationWhitelist", whitelist)
            .doesNotContainKeys("entries", "value");

        AuthConfig propertiesCopy = new AuthConfig();
        MixAll.properties2Object(properties, propertiesCopy);
        assertThat(propertiesCopy.getAuthenticationWhitelist()).isEqualTo(whitelist.trim());
        assertThat(propertiesCopy.isAuthenticationRequired("AUTH_A")).isFalse();

        AuthConfig jsonCopy = JSON.parseObject(JSON.toJSONString(authConfig), AuthConfig.class);
        assertThat(jsonCopy.getAuthenticationWhitelist()).isEqualTo(whitelist);
        assertThat(jsonCopy.isAuthenticationRequired("AUTH_B")).isFalse();

        AuthConfig cloned = authConfig.clone();
        authConfig.setAuthenticationWhitelist("AUTH_C");
        assertThat(cloned.isAuthenticationRequired("AUTH_A")).isFalse();
        assertThat(cloned.isAuthenticationRequired("AUTH_C")).isTrue();
    }
}
