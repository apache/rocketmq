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
package org.apache.rocketmq.tools.command.broker;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class ConfigUpdateSupport {
    public enum BrokerConfigKey {
        messageDelayLevel,
        brokerPermission
        // ...  all other config keys have been sorted out and iterated here.
    }

    public enum UpdateSupportResEnum {
        CONFIG_UPDATED_BROKER_LOADED("config takes effect and the broker takes effect dynamically without restarting."),
        ONLY_CONFIG_UPDATED("only config is updated, but this config cann't take effect dynamically, " +
                "you need to be restarted to take effect."),
        UNDEFINED("");
        private final String res;

        UpdateSupportResEnum(String res) {
            this.res = res;
        }
    }

    private static final Map<BrokerConfigKey /* BrokerConfigKey*/, UpdateSupportResEnum> SUPPORT_RES_ENUM_MAP
            = ImmutableMap.<BrokerConfigKey, UpdateSupportResEnum>builder()
            .put(BrokerConfigKey.messageDelayLevel, UpdateSupportResEnum.ONLY_CONFIG_UPDATED)
            .put(BrokerConfigKey.brokerPermission, UpdateSupportResEnum.CONFIG_UPDATED_BROKER_LOADED)
            // ...  all other configs have been sorted out and iterated here.
            .build();

    public static UpdateSupportResEnum getUpdateSupportResEnumByKey(String configKey) {
        try {
            BrokerConfigKey keyEnum = BrokerConfigKey.valueOf(configKey);
            return SUPPORT_RES_ENUM_MAP.getOrDefault(keyEnum, UpdateSupportResEnum.UNDEFINED);
        } catch (IllegalArgumentException e) {
            System.out.printf("The current key %s has not been maintained yet.", configKey);
        }
        return UpdateSupportResEnum.UNDEFINED;
    }
}
