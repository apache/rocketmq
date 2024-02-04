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

package org.apache.rocketmq.broker.plugin;

import java.util.Map;

public interface BrokerAttachedPlugin {

    /**
     * Get plugin name
     *
     * @return plugin name
     */
    String pluginName();

    /**
     * Load broker attached plugin.
     *
     * @return load success or failed
     */
    boolean load();

    /**
     * Start broker attached plugin.
     */
    void start();

    /**
     * Shutdown broker attached plugin.
     */
    void shutdown();

    /**
     * Sync metadata from master.
     */
    void syncMetadata();

    /**
     * Sync metadata reverse from slave
     *
     * @param brokerAddr
     */
    void syncMetadataReverse(String brokerAddr) throws Exception;

    /**
     * Some plugin need build runningInfo when prepare runtime info.
     *
     * @param runtimeInfo
     */
    void buildRuntimeInfo(Map<String, String> runtimeInfo);

    /**
     * Some plugin need do something when status changed. For example, brokerRole change to master or slave.
     *
     * @param shouldStart
     */
    void statusChanged(boolean shouldStart);

}
