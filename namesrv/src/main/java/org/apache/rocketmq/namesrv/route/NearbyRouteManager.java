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
package org.apache.rocketmq.namesrv.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.route.NearbyRoute;
import org.apache.rocketmq.common.route.Network;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class NearbyRouteManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    public static final NearbyRouteManager INSTANCE = new NearbyRouteManager();

    private volatile NearbyRoute nearbyRoute = new NearbyRoute();

    private String configFilePath = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV)) + "/conf/nearby_route.yml";

    private NearbyRouteManager() {}

    public void load(String ymlFilePath) {
        configFilePath = ymlFilePath;
        boolean result = super.load();
        if (!result) {
            log.error("fail to load topic nearby route yaml file {}", ymlFilePath);
        }
    }

    public boolean isNearbyRoute() {
        return nearbyRoute.isNearbyRoute();
    }

    public boolean isWhiteRemoteAddresses(String remoteAddr) {
        return match(remoteAddr, nearbyRoute.getWhiteRemoteAddresses());
    }

    public TopicRouteData filter(String remoteAddr, TopicRouteData topicRouteData) {
        Network network = null;
        //Remote address is not included in all network strategy, Do not filter.
        if ((network = match(remoteAddr)) == null) {
            return topicRouteData;
        }
        List<BrokerData> brokerDataReserved = new ArrayList<>();
        Map<String, BrokerData> brokerDataRemoved = new HashMap<>();
        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            //master down, consume from slave. break nearby route rule.
            if (brokerData.getBrokerAddrs().get(MixAll.MASTER_ID) == null
                || match(brokerData.selectBrokerAddr(), network.getSubnet())) {
                brokerDataReserved.add(brokerData);
            } else {
                brokerDataRemoved.put(brokerData.getBrokerName(), brokerData);
            }
        }
        topicRouteData.setBrokerDatas(brokerDataReserved);

        List<QueueData> queueDataReserved = new ArrayList<>();
        for (QueueData queueData : topicRouteData.getQueueDatas()) {
            if (!brokerDataRemoved.containsKey(queueData.getBrokerName())) {
                queueDataReserved.add(queueData);
            }
        }
        topicRouteData.setQueueDatas(queueDataReserved);
        // remove filter server table by broker address
        if (topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            for (Entry<String, BrokerData> entry : brokerDataRemoved.entrySet()) {
                BrokerData brokerData = entry.getValue();
                if (brokerData.getBrokerAddrs() == null) {
                    continue;
                }
                brokerData.getBrokerAddrs().values()
                    .stream()
                    .forEach(brokerAddr -> topicRouteData.getFilterServerTable().remove(brokerAddr));
            }
        }
        return topicRouteData;
    }

    private Network match(String ip) {
        return nearbyRoute.getNetworks().stream()
                .filter(network -> match(ip, network.getSubnet()))
                .findFirst()
                .orElse(null);
    }

    private boolean match(String ip, List<String> subnets) {
        boolean result = false;
        for (String subnet : subnets) {
            String segment = StringUtils.substringBefore(subnet, "*");
            if (StringUtils.startsWith(ip, segment)) {
                result = true;
                break;
            }
        }
        return result;
    }

    @Override
    public String encode() {
        Yaml yaml = new Yaml();
        return yaml.dump(nearbyRoute);
    }

    @Override
    public String configFilePath() {
        return configFilePath;
    }

    @Override
    public void decode(String yml) {
        Yaml yaml = new Yaml();
        this.nearbyRoute = yaml.loadAs(yml, NearbyRoute.class);
    }

    @Override
    public String encode(boolean prettyFormat) {
        return encode();
    }

    /**
     * Update nearby route, then persist to yaml file.
     * @param nearbyRoute
     */
    public void updateNearbyRoute(NearbyRoute nearbyRoute) {
        this.nearbyRoute = nearbyRoute;
        this.persist();
    }

    public NearbyRoute getNearbyRoute() {
        return this.nearbyRoute;
    }
}
