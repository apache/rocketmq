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

    // <clusterName, brokerAddr>, Used to judge whether a broker
    // has preliminary qualification to be selected as master
    private BiPredicate<String, String> validPredicate;

    // <clusterName, brokerAddr, BrokerLiveInfo>, Used to obtain the BrokerLiveInfo information of a broker
    private BiFunction<String, String, BrokerLiveInfo> additionalInfoGetter;

    // Sort in descending order according to<epoch, offset>, and sort in ascending order according to priority
    private final Comparator<BrokerLiveInfo> comparator = (o1, o2) -> {
        if (o1.getEpoch() == o2.getEpoch()) {
            return o1.getMaxOffset() == o2.getMaxOffset() ? o1.getElectionPriority() - o2.getElectionPriority() :
                    (int) (o2.getMaxOffset() - o1.getMaxOffset());
        } else {
            return o2.getEpoch() - o1.getEpoch();
        }
    };

    public DefaultElectPolicy(BiPredicate<String, String> validPredicate, BiFunction<String, String, BrokerLiveInfo> additionalInfoGetter) {
        this.validPredicate = validPredicate;
        this.additionalInfoGetter = additionalInfoGetter;
    }

    public DefaultElectPolicy() {

    }

    /**
     * We will try to select a new master from syncStateBrokers and allReplicaBrokers in turn.
     * The strategies are as follows:
     *    - Filter alive brokers by 'validPredicate'.
     *    - Check whether the old master is still valid.
     *    - If preferBrokerAddr is not empty and valid, select it as master.
     *    - Otherwise, we will sort the array of 'brokerLiveInfo' according to (epoch, offset, electionPriority), and select the best candidate as the new master.
     *
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

        // try to elect in all allReplicaBrokers
        if (allReplicaBrokers != null) {
            newMaster = tryElect(clusterName, allReplicaBrokers, oldMaster, preferBrokerAddr);
        }
        return newMaster;
    }


    private String tryElect(String clusterName, Set<String> brokers, String oldMaster, String preferBrokerAddr) {
        if (this.validPredicate != null) {
            brokers = brokers.stream().filter(brokerAddr -> this.validPredicate.test(clusterName, brokerAddr)).collect(Collectors.toSet());
        }
        if (!brokers.isEmpty()) {
            // if old master is still valid, and preferBrokerAddr is blank or is equals to oldMaster
            if (brokers.contains(oldMaster) && (StringUtils.isBlank(preferBrokerAddr) || preferBrokerAddr.equals(oldMaster))) {
                return oldMaster;
            }

            // if preferBrokerAddr is valid, we choose it, otherwise we choose nothing
            if (StringUtils.isNotBlank(preferBrokerAddr)) {
                return brokers.contains(preferBrokerAddr) ? preferBrokerAddr : null;
            }

            if (this.additionalInfoGetter != null) {
                // sort brokerLiveInfos by (epoch,maxOffset)
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
