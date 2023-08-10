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
package org.apache.rocketmq.controller.elect;


import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.controller.heartbeat.BrokerLiveInfo;

public interface ElectPolicy {

    /**
     * elect a master
     *
     * @param syncStateBrokers        all broker replicas in syncStateSet
     * @param allReplicaBrokers       all broker replicas
     * @param aliveReplicaBrokersInfo all alive broker replicas info
     * @param oldMaster               old master
     * @param extraBrokerId           an extra broker id(can be used as prefer or assigned in some elect policy)
     * @return new master's broker id
     */
    Long elect(Set<Long> syncStateBrokers, Set<Long> allReplicaBrokers, Map<Long/*broker id*/, BrokerLiveInfo> aliveReplicaBrokersInfo,
        Long oldMaster, Long extraBrokerId);

}
