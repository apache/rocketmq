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
package org.apache.rocketmq.tools.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.MQAdminExt;

public class CommandUtil {

    public static Map<String/*master addr*/, List<String>/*slave addr*/> fetchMasterAndSlaveDistinguish(
        final MQAdminExt adminExt, final String clusterName)
        throws InterruptedException, RemotingConnectException,
        RemotingTimeoutException, RemotingSendRequestException,
        MQBrokerException {
        Map<String, List<String>> masterAndSlaveMap = new HashMap<String, List<String>>(4);

        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);

        if (brokerNameSet == null) {
            System.out
                .printf("[error] Make sure the specified clusterName exists or the nameserver which connected is correct.");
            return masterAndSlaveMap;
        }

        for (String brokerName : brokerNameSet) {
            BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);

            if (brokerData == null || brokerData.getBrokerAddrs() == null) {
                continue;
            }

            String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            masterAndSlaveMap.put(masterAddr, new ArrayList<String>());

            for (Long id : brokerData.getBrokerAddrs().keySet()) {
                if (brokerData.getBrokerAddrs().get(id) == null
                    || id.longValue() == MixAll.MASTER_ID) {
                    continue;
                }

                masterAndSlaveMap.get(masterAddr).add(brokerData.getBrokerAddrs().get(id));
            }
        }

        return masterAndSlaveMap;
    }

    public static Set<String> fetchMasterAddrByClusterName(final MQAdminExt adminExt, final String clusterName)
        throws InterruptedException, RemotingConnectException, RemotingTimeoutException,
        RemotingSendRequestException, MQBrokerException {
        Set<String> masterSet = new HashSet<String>();

        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();

        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);

        if (brokerNameSet != null) {
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {

                    String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (addr != null) {
                        masterSet.add(addr);
                    }
                }
            }
        } else {
            System.out
                .printf("[error] Make sure the specified clusterName exists or the nameserver which connected is correct.");
        }

        return masterSet;
    }

    public static Set<String> fetchMasterAndSlaveAddrByClusterName(final MQAdminExt adminExt, final String clusterName)
        throws InterruptedException, RemotingConnectException, RemotingTimeoutException,
        RemotingSendRequestException, MQBrokerException {
        Set<String> masterSet = new HashSet<String>();

        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();

        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);

        if (brokerNameSet != null) {
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {
                    final Collection<String> addrs = brokerData.getBrokerAddrs().values();
                    masterSet.addAll(addrs);
                }
            }
        } else {
            System.out
                .printf("[error] Make sure the specified clusterName exists or the nameserver which connected is correct.");
        }

        return masterSet;
    }

    public static Set<String> fetchBrokerNameByClusterName(final MQAdminExt adminExt, final String clusterName)
        throws Exception {
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
        if (brokerNameSet.isEmpty()) {
            throw new Exception(
                "Make sure the specified clusterName exists or the nameserver which connected is correct.");
        }
        return brokerNameSet;
    }

    public static String fetchBrokerNameByAddr(final MQAdminExt adminExt, final String addr) throws Exception {
        ClusterInfo clusterInfoSerializeWrapper = adminExt.examineBrokerClusterInfo();
        HashMap<String/* brokerName */, BrokerData> brokerAddrTable =
            clusterInfoSerializeWrapper.getBrokerAddrTable();
        Iterator<Map.Entry<String, BrokerData>> it = brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BrokerData> entry = it.next();
            HashMap<Long, String> brokerAddrs = entry.getValue().getBrokerAddrs();
            if (brokerAddrs.containsValue(addr))
                return entry.getKey();
        }
        throw new Exception(
            "Make sure the specified broker addr exists or the nameserver which connected is correct.");
    }

}
