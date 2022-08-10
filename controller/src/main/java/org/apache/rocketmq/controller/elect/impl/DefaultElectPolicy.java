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
package org.apache.rocketmq.controller.elect.impl;

import io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.pojo.BrokerLiveInfo;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class DefaultElectPolicy extends ElectPolicy {

    private final Comparator<BrokerLiveInfo> comparator;

    public DefaultElectPolicy(BiPredicate<String, String> validPredicate, BiFunction<String, String, BrokerLiveInfo> additionalInfoGetter) {
        super(validPredicate, additionalInfoGetter);
        comparator = (x, y) -> {
            return x.getEpoch() == y.getEpoch() ? (int) (y.getMaxOffset() - x.getMaxOffset()) : y.getEpoch() - x.getEpoch();
        };
    }


    @Override
    public String elect(String clusterName, Set<String> syncStateBrokers, Set<String> allReplicaBrokers, String oldMaster) {
        String newMaster = null;
        // try to elect in syncStateBrokers
        if (syncStateBrokers != null) {
            newMaster = tryElect(clusterName, syncStateBrokers, oldMaster);
        }
        if (StringUtils.isNotEmpty(newMaster)) {
            return newMaster;
        }
        // try to elect in all replicas
        if (allReplicaBrokers != null) {
            newMaster = tryElect(clusterName, allReplicaBrokers, oldMaster);
        }
        return newMaster;
    }

    private String tryElect(String clusterName, Set<String> brokers, String oldMaster) {
        if (super.validPredicate != null) {
            brokers = brokers.stream().filter(brokerAddr -> super.validPredicate.test(clusterName, brokerAddr)).collect(Collectors.toSet());
        }
        // try to elect in brokers
        if (brokers.size() >= 1) {
            if (brokers.contains(oldMaster)) {
                // old master still valid, we prefer it
                return oldMaster;
            }
            if (super.additionalInfoGetter != null) {
                // get more information from getter
                BiFunction<String, String, BrokerLiveInfo> getter = (BiFunction<String, String, BrokerLiveInfo>) super.additionalInfoGetter;
                // sort brokerLiveInfos by epoch, maxOffset
                TreeSet<BrokerLiveInfo> brokerLiveInfos = new TreeSet<>(this.comparator);
                brokers.forEach(brokerAddr -> brokerLiveInfos.add(getter.apply(clusterName, brokerAddr)));
                if (brokerLiveInfos.size() >= 1) {
                    return brokerLiveInfos.first().getBrokerAddr();
                }
            }
            // elect random
            return brokers.iterator().next();
        }
        return null;
    }
}
