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
package org.apache.rocketmq.controller;

import io.netty.channel.Channel;

public interface BrokerHeartbeatManager {

    /**
     * Broker new heartbeat.
     */
    void onBrokerHeartbeat(final String clusterName, final String brokerAddr, final Integer epoch, final Long maxOffset, final Long confirmOffset);

    /**
     * Change the metadata(brokerId ..) for a broker.
     */
    void changeBrokerMetadata(final String clusterName, final String brokerAddr, final Long brokerId);

    /**
     * Start heartbeat manager.
     */
    void start();

    /**
     * Shutdown heartbeat manager.
     */
    void shutdown();

    /**
     * Add BrokerLifecycleListener.
     */
    void addBrokerLifecycleListener(final BrokerLifecycleListener listener);

    /**
     * Register new broker to heartManager.
     */
    void registerBroker(final String clusterName, final String brokerName, final String brokerAddr, final long brokerId,
                        final Long timeoutMillis, final Channel channel, final Integer epoch, final Long maxOffset);

    /**
     * Broker channel close
     */
    void onBrokerChannelClose(final Channel channel);

    /**
     * Get broker live information by clusterName and brokerAddr
     */
    BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerAddr);

    /**
     * Check whether broker active
     */
    boolean isBrokerActive(final String clusterName, final String brokerAddr);

    interface BrokerLifecycleListener {
        /**
         * Trigger when broker inactive.
         */
        void onBrokerInactive(final String clusterName, final String brokerName, final String brokerAddress, final long brokerId);
    }
}
