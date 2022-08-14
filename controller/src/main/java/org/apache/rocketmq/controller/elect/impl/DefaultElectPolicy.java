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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.BrokerLiveInfo;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class DefaultElectPolicy implements ElectPolicy {

    // <clusterName, brokerAddr> valid predicate
    private BiPredicate<String, String> validPredicate;

    // <clusterName, brokerAddr, info> getter to get more information
    private BiFunction<String, String, BrokerLiveInfo> additionalInfoGetter;

    private final Comparator<BrokerLiveInfo> comparator = (x, y) -> {
        return x.getEpoch() == y.getEpoch() ? (int) (y.getMaxOffset() - x.getMaxOffset()) : y.getEpoch() - x.getEpoch();
    };

    public DefaultElectPolicy(BiPredicate<String, String> validPredicate, BiFunction<String, String, BrokerLiveInfo> additionalInfoGetter) {
        this.validPredicate = validPredicate;
        this.additionalInfoGetter = additionalInfoGetter;
    }

    public DefaultElectPolicy() {

    }

    /**
     * try to elect a master, if old master still alive, now we do nothing,
     * if preferBrokerAddr is not blank, that means we must elect a new master,
     * and we should check if the preferBrokerAddr is valid, if so we should elect it as
     * new master, if else we should elect nothing.
     * @param clusterName       the brokerGroup belongs
     * @param syncStateBrokers  all broker replicas in syncStateSet
     * @param allReplicaBrokers all broker replicas
     * @param oldMaster         old master
     * @param preferBrokerAddr  the broker prefer to be elected
     * @return master elected by our own policy
     */
    @Override
    public String elect(String clusterName, Set<String> syncStateBrokers, Set<String> allReplicaBrokers, String oldMaster, String preferBrokerAddr) {
        String newMaster = null;
        // try to elect in syncStateBrokers
        if (syncStateBrokers != null) {
            newMaster = tryElect(clusterName, syncStateBrokers, oldMaster, preferBrokerAddr);
        }
        if (StringUtils.isNotEmpty(newMaster)) {
            return newMaster;
        }
        // try to elect in all replicas
        if (allReplicaBrokers != null) {
            newMaster = tryElect(clusterName, allReplicaBrokers, oldMaster, preferBrokerAddr);
        }
        return newMaster;
    }


    private String tryElect(String clusterName, Set<String> brokers, String oldMaster, String preferBrokerAddr) {
        if (this.validPredicate != null) {
            brokers = brokers.stream().filter(brokerAddr -> this.validPredicate.test(clusterName, brokerAddr)).collect(Collectors.toSet());
        }
        // try to elect in brokers
        if (brokers.size() >= 1) {
            if (brokers.contains(oldMaster) && (StringUtils.isBlank(preferBrokerAddr) || preferBrokerAddr.equals(oldMaster))) {
                // old master still valid, and our preferBrokerAddr is blank or is equals to oldMaster
                return oldMaster;
            }
            // if preferBrokerAddr is not blank, if preferBrokerAddr is valid, we choose it, else we choose nothing
            if (StringUtils.isNotBlank(preferBrokerAddr)) {
                return brokers.contains(preferBrokerAddr) ? preferBrokerAddr : null;
            }
            if (this.additionalInfoGetter != null) {
                // get more information from getter
                // sort brokerLiveInfos by epoch, maxOffset
                TreeSet<BrokerLiveInfo> brokerLiveInfos = new TreeSet<>(this.comparator);
                brokers.forEach(brokerAddr -> brokerLiveInfos.add(this.additionalInfoGetter.apply(clusterName, brokerAddr)));
                if (brokerLiveInfos.size() >= 1) {
                    return brokerLiveInfos.first().getBrokerAddr();
                }
            }
            // elect random
            return brokers.iterator().next();
        }
        return null;
    }


    public BiFunction<String, String, BrokerLiveInfo> getAdditionalInfoGetter() {
        return additionalInfoGetter;
    }

    public void setAdditionalInfoGetter(BiFunction<String, String, BrokerLiveInfo> additionalInfoGetter) {
        this.additionalInfoGetter = additionalInfoGetter;
    }

    public BiPredicate<String, String> getValidPredicate() {
        return validPredicate;
    }

    public void setValidPredicate(BiPredicate<String, String> validPredicate) {
        this.validPredicate = validPredicate;
    }
}
