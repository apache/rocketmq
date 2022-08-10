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

import org.apache.rocketmq.controller.pojo.BrokerLiveInfo;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

public abstract class ElectPolicy {

    // <clusterName, brokerAddr> valid predicate
    protected BiPredicate<String, String> validPredicate;

    // <clusterName, brokerAddr, info> getter to get more information
    protected BiFunction<String, String, ?> additionalInfoGetter;

    public ElectPolicy(BiPredicate<String, String> validPredicate, BiFunction<String, String, BrokerLiveInfo> additionalInfoGetter) {
        this.validPredicate = validPredicate;
        this.additionalInfoGetter = additionalInfoGetter;
    }

    /**
     * elect a master
     * @param clusterName the brokerGroup belongs
     * @param syncStateBrokers all broker replicas in syncStateSet
     * @param allReplicaBrokers all broker replicas
     * @param oldMaster old master
     * @return new master's brokerAddr
     */
    public abstract String elect(String clusterName, Set<String> syncStateBrokers, Set<String> allReplicaBrokers, String oldMaster);
}
