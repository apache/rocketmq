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
package org.apache.rocketmq.remoting.protocol.header;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.EXPORT_ROCKSDB_CONFIG_TO_JSON, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class ExportRocksDBConfigToJsonRequestHeader implements CommandCustomHeader {
    private static final String CONFIG_TYPE_SEPARATOR = ";";

    public enum ConfigType {
        TOPICS("topics"),
        SUBSCRIPTION_GROUPS("subscriptionGroups"),
        CONSUMER_OFFSETS("consumerOffsets");

        private final String typeName;

        ConfigType(String typeName) {
            this.typeName = typeName;
        }

        public static ConfigType getConfigTypeByName(String typeName) {
            for (ConfigType configType : ConfigType.values()) {
                if (configType.getTypeName().equalsIgnoreCase(typeName.trim())) {
                    return configType;
                }
            }
            throw new IllegalArgumentException("Unknown config type: " + typeName);
        }

        public static List<ConfigType> fromString(String ordinal) {
            String[] configTypeNames = StringUtils.split(ordinal, CONFIG_TYPE_SEPARATOR);
            List<ConfigType> configTypes = new ArrayList<>();
            for (String configTypeName : configTypeNames) {
                if (StringUtils.isNotEmpty(configTypeName)) {
                    configTypes.add(getConfigTypeByName(configTypeName));
                }
            }
            return configTypes;
        }

        public static String toString(List<ConfigType> configTypes) {
            StringBuilder sb = new StringBuilder();
            for (ConfigType configType : configTypes) {
                sb.append(configType.getTypeName()).append(CONFIG_TYPE_SEPARATOR);
            }
            return sb.toString();
        }

        public String getTypeName() {
            return typeName;
        }
    }

    @CFNotNull
    private String configType;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public List<ConfigType> fetchConfigType() {
        return ConfigType.fromString(configType);
    }

    public void updateConfigType(List<ConfigType> configType) {
        this.configType = ConfigType.toString(configType);
    }

    public String getConfigType() {
        return configType;
    }

    public void setConfigType(String configType) {
        this.configType = configType;
    }
}