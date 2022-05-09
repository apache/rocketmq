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
package org.apache.rocketmq.namesrv.controller;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * The api for controller
 */
public interface Controller {

    /**
     * Startup controller
     */
    void startup();

    /**
     * Shutdown controller
     */
    void shutdown();

    /**
     * Start scheduling controller events, this function only will be triggered when the controller becomes leader.
     */
    void startScheduling();

    /**
     * Stop scheduling controller events, this function only will be triggered when the controller shutdown leaderShip.
     */
    void stopScheduling();

    /**
     * Alter ISR of broker replicas.
     *
     * @param request AlterSyncStateSetRequestHeader
     * @return RemotingCommand(AlterSyncStateSetResponseHeader)
     */
    CompletableFuture<RemotingCommand> alterSyncStateSet(
        final AlterSyncStateSetRequestHeader request, final SyncStateSet syncStateSet);

    /**
     * Elect new master for a broker.
     *
     * @param request ElectMasterRequest
     * @return RemotingCommand(ElectMasterResponseHeader)
     */
    CompletableFuture<RemotingCommand> electMaster(final ElectMasterRequestHeader request);

    /**
     * Register api when a replicas of a broker startup.
     *
     * @param request RegisterBrokerRequest
     * @return RemotingCommand(RegisterBrokerResponseHeader)
     */
    CompletableFuture<RemotingCommand> registerBroker(final RegisterBrokerRequestHeader request);

    /**
     * Get the Replica Info for a target broker.
     *
     * @param request GetRouteInfoRequest
     * @return RemotingCommand(GetReplicaInfoResponseHeader)
     */
    CompletableFuture<RemotingCommand> getReplicaInfo(final GetReplicaInfoRequestHeader request);

    /**
     * Get Metadata of controller
     *
     * @return RemotingCommand(GetControllerMetadataResponseHeader)
     */
    RemotingCommand getControllerMetadata();
}
