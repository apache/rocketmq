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

package org.apache.rocketmq.container;

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * An interface for broker container to hold multiple master and slave brokers.
 */
public interface IBrokerContainer {

    /**
     * Start broker container
     */
    void start() throws Exception;

    /**
     * Shutdown broker container and all the brokers inside.
     */
    void shutdown();

    /**
     * Add a broker to this container with specific broker config.
     *
     * @param brokerConfig the specified broker config
     * @param storeConfig the specified store config
     * @return the added BrokerController or null if the broker already exists
     * @throws Exception when initialize broker
     */
    BrokerController addBroker(BrokerConfig brokerConfig, MessageStoreConfig storeConfig) throws Exception;

    /**
     * Remove the broker from this container associated with the specific broker identity
     *
     * @param brokerIdentity the specific broker identity
     * @return the removed BrokerController or null if the broker doesn't exists
     */
    BrokerController removeBroker(BrokerIdentity brokerIdentity) throws Exception;

    /**
     * Return the broker controller associated with the specific broker identity
     *
     * @param brokerIdentity the specific broker identity
     * @return the associated messaging broker or null
     */
    BrokerController getBroker(BrokerIdentity brokerIdentity);

    /**
     * Return all the master brokers belong to this container
     *
     * @return the master broker list
     */
    Collection<InnerBrokerController> getMasterBrokers();

    /**
     * Return all the slave brokers belong to this container
     *
     * @return the slave broker list
     */
    Collection<InnerSalveBrokerController> getSlaveBrokers();

    /**
     * Return all broker controller in this container
     *
     * @return all broker controller
     */
    List<BrokerController> getBrokerControllers();

    /**
     * Return the address of broker container.
     *
     * @return broker container address.
     */
    String getBrokerContainerAddr();

    /**
     * Peek the first master broker in container.
     *
     * @return the first master broker in container
     */
    BrokerController peekMasterBroker();

    /**
     * Return the config of the broker container
     *
     * @return the broker container config
     */
    BrokerContainerConfig getBrokerContainerConfig();

    /**
     * Get netty server config.
     *
     * @return netty server config
     */
    NettyServerConfig getNettyServerConfig();

    /**
     * Get netty client config.
     *
     * @return netty client config
     */
    NettyClientConfig getNettyClientConfig();

    /**
     * Return the shared BrokerOuterAPI
     *
     * @return the shared BrokerOuterAPI
     */
    BrokerOuterAPI getBrokerOuterAPI();

    /**
     * Return the shared RemotingServer
     *
     * @return the shared RemotingServer
     */
    RemotingServer getRemotingServer();
}
