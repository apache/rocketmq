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
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.controller.heartbeat.BrokerSetIdentity;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.controller.heartbeat.BrokerLiveInfo;

public interface BrokerHeartbeatManager {

    /**
     * initialize the resources
     * @return
     */
    void initialize();
    /**
     * Broker new heartbeat.
     */
    void onBrokerHeartbeat(final String clusterName, final String brokerName, final String brokerAddr,
        final Long brokerId, final Long timeoutMillis, final Channel channel, final Integer epoch,
        final Long maxOffset, final Long confirmOffset, final Integer electionPriority);

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
    void registerBrokerLifecycleListener(final BrokerLifecycleListener listener);

    /**
     * Broker channel close
     */
    void onBrokerChannelClose(final Channel channel);

    /**
     * Get broker live information by clusterName and brokerAddr
     */
    BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerName, Long brokerId);

    /**
     * Check whether broker active
     */
    boolean isBrokerActive(final String clusterName, final String brokerName, final Long brokerId);

    /**
     * Count the number of active brokers in each broker-set of each cluster
     * @return active brokers count
     */
    Map<BrokerSetIdentity, Integer/*brokers num*/> getActiveBrokersNum();

    /**
     * Get all active broker ids for target broker-set
     * @param clusterName cluster name
     * @param brokerName broker name
     * @return broker id set which is active
     */
    Set<Long> getActiveBrokerIds(String clusterName, String brokerName);
}
