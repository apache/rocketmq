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

package org.apache.rocketmq.common.config;

/**
 * Version identifier for the persistent config file format used by {@code ConfigManager}.
 *
 * <p>{@link #V1} is the legacy JSON format where the config is encoded directly as a
 * JSON string ({@code JSON.toJSONString(obj)}).
 *
 * <p>{@link #V2} stores additional metadata (version, encoding) alongside the serialized config,
 * enabling forward-compatible config migration.
 */
public enum ConfigManagerVersion {
    V1("v1"),
    V2("v2"),
    ;
    private final String version;

    ConfigManagerVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
