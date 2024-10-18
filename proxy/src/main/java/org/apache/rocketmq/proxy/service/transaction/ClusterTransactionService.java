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
package org.apache.rocketmq.proxy.service.transaction;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

public class ClusterTransactionService extends AbstractTransactionService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final String TRANS_HEARTBEAT_CLIENT_ID = "rmq-proxy-producer-client";

    private final MQClientAPIFactory mqClientAPIFactory;
    private final TopicRouteService topicRouteService;
    private final ProducerManager producerManager;

    private ThreadPoolExecutor heartbeatExecutors;
    private final Map<String /* group */, ClusterDataSet> groupClusterData = new ConcurrentHashMap<>();
    private final AtomicReference<Map<String /* brokerAddr */, String /* brokerName */>> brokerAddrNameMapRef = new AtomicReference<>();
    private TxHeartbeatServiceThread txHeartbeatServiceThread;

    public ClusterTransactionService(TopicRouteService topicRouteService, ProducerManager producerManager,
        MQClientAPIFactory mqClientAPIFactory) {
        this.topicRouteService = topicRouteService;
        this.producerManager = producerManager;
        this.mqClientAPIFactory = mqClientAPIFactory;
    }

    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
        for (String topic : topicList) {
            addTransactionSubscription(ctx, group, topic);
        }
    }

    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, String topic) {
        try {
            groupClusterData.compute(group, (groupName, clusterDataSet) -> {
                if (clusterDataSet == null) {
                    clusterDataSet = new ClusterDataSet();
                }
                clusterDataSet.addAll(getClusterDataFromTopic(ctx, topic));
                return clusterDataSet;
            });
        } catch (Exception e) {
            log.error("add producer group err in txHeartBeat. groupId: {}, err: {}", group, e);
        }
    }

    @Override
    public void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
        ClusterDataSet clusterDataSet = new ClusterDataSet();
        for (String topic : topicList) {
            clusterDataSet.addAll(getClusterDataFromTopic(ctx, topic));
        }
        groupClusterData.put(group, clusterDataSet);
    }

    private Set<ClusterData> getClusterDataFromTopic(ProxyContext ctx, String topic) {
        try {
            MessageQueueView messageQueue = this.topicRouteService.getAllMessageQueueView(ctx, topic);
            List<BrokerData> brokerDataList = messageQueue.getTopicRouteData().getBrokerDatas();

            if (brokerDataList == null) {
                return Collections.emptySet();
            }
            Set<ClusterData> res = Sets.newHashSet();
            for (BrokerData brokerData : brokerDataList) {
                res.add(new ClusterData(brokerData.getCluster()));
            }
            return res;
        } catch (Throwable t) {
            log.error("get cluster data failed in txHeartBeat. topic: {}, err: {}", topic, t);
        }
        return Collections.emptySet();
    }

    @Override
    public void unSubscribeAllTransactionTopic(ProxyContext ctx, String group) {
        groupClusterData.remove(group);
    }

    public void scanProducerHeartBeat() {
        long offlineTimout = ConfigurationManager.getProxyConfig().getTransactionGroupOfflineTimeoutMillis();
        Set<String> groupSet = groupClusterData.keySet();

        Map<String /* cluster */, List<HeartbeatData>> clusterHeartbeatData = new HashMap<>();
        for (String group : groupSet) {
            groupClusterData.computeIfPresent(group, (groupName, clusterDataSet) -> {
                if (clusterDataSet.getSet().isEmpty()) {
                    return null;
                }
                // Whether the group is offline should be determined comprehensively based on client heartbeats
                // and last addTransactionSubscription timestamp, in case the producer sent all messages before its first heartbeat.
                if (!this.producerManager.groupOnline(groupName) && clusterDataSet.getLastAddTimestamp() + offlineTimout < System.currentTimeMillis()) {
                    return null;
                }

                ProducerData producerData = new ProducerData();
                producerData.setGroupName(groupName);

                for (ClusterData clusterData : clusterDataSet.getSet()) {
                    List<HeartbeatData> heartbeatDataList = clusterHeartbeatData.get(clusterData.cluster);
                    if (heartbeatDataList == null) {
                        heartbeatDataList = new ArrayList<>();
                    }

                    HeartbeatData heartbeatData;
                    if (heartbeatDataList.isEmpty()) {
                        heartbeatData = new HeartbeatData();
                        heartbeatData.setClientID(TRANS_HEARTBEAT_CLIENT_ID);
                        heartbeatDataList.add(heartbeatData);
                    } else {
                        heartbeatData = heartbeatDataList.get(heartbeatDataList.size() - 1);
                        if (heartbeatData.getProducerDataSet().size() >= ConfigurationManager.getProxyConfig().getTransactionHeartbeatBatchNum()) {
                            heartbeatData = new HeartbeatData();
                            heartbeatData.setClientID(TRANS_HEARTBEAT_CLIENT_ID);
                            heartbeatDataList.add(heartbeatData);
                        }
                    }

                    heartbeatData.getProducerDataSet().add(producerData);
                    clusterHeartbeatData.put(clusterData.cluster, heartbeatDataList);
                }

                if (clusterDataSet.getSet().isEmpty()) {
                    return null;
                }
                return clusterDataSet;
            });
        }

        if (clusterHeartbeatData.isEmpty()) {
            return;
        }
        Map<String, String> brokerAddrNameMap = new ConcurrentHashMap<>();
        Set<Map.Entry<String, List<HeartbeatData>>> clusterEntry = clusterHeartbeatData.entrySet();
        for (Map.Entry<String, List<HeartbeatData>> entry : clusterEntry) {
            sendHeartBeatToCluster(entry.getKey(), entry.getValue(), brokerAddrNameMap);
        }
        this.brokerAddrNameMapRef.set(brokerAddrNameMap);
    }

    public Map<String, ClusterDataSet> getGroupClusterData() {
        return groupClusterData;
    }

    protected void sendHeartBeatToCluster(String clusterName, List<HeartbeatData> heartbeatDataList, Map<String, String> brokerAddrNameMap) {
        if (heartbeatDataList == null) {
            return;
        }
        for (HeartbeatData heartbeatData : heartbeatDataList) {
            sendHeartBeatToCluster(clusterName, heartbeatData, brokerAddrNameMap);
        }
        this.brokerAddrNameMapRef.set(brokerAddrNameMap);
    }

    protected void sendHeartBeatToCluster(String clusterName, HeartbeatData heartbeatData, Map<String, String> brokerAddrNameMap) {
        try {
            MessageQueueView messageQueue = this.topicRouteService.getAllMessageQueueView(ProxyContext.createForInner(this.getClass()), clusterName);
            List<BrokerData> brokerDataList = messageQueue.getTopicRouteData().getBrokerDatas();
            if (brokerDataList == null) {
                return;
            }
            for (BrokerData brokerData : brokerDataList) {
                brokerAddrNameMap.put(brokerData.selectBrokerAddr(), brokerData.getBrokerName());
                heartbeatExecutors.submit(() -> {
                    String brokerAddr = brokerData.selectBrokerAddr();
                    this.mqClientAPIFactory.getClient()
                        .sendHeartbeatOneway(brokerAddr, heartbeatData, Duration.ofSeconds(3).toMillis())
                        .exceptionally(t -> {
                            log.error("Send transactionHeartbeat to broker err. brokerAddr: {}", brokerAddr, t);
                            return null;
                        });
                });
            }
        } catch (Exception e) {
            log.error("get broker add in cluster failed in tx. clusterName: {}", clusterName, e);
        }
    }

    @Override
    protected String getBrokerNameByAddr(String brokerAddr) {
        if (StringUtils.isBlank(brokerAddr)) {
            return null;
        }
        return brokerAddrNameMapRef.get().get(brokerAddr);
    }

    static class ClusterData {
        private final String cluster;

        public ClusterData(String cluster) {
            this.cluster = cluster;
        }

        public String getCluster() {
            return cluster;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof ClusterData)) {
                return super.equals(obj);
            }

            ClusterData other = (ClusterData) obj;
            return cluster.equals(other.cluster);
        }

        @Override
        public int hashCode() {
            return cluster.hashCode();
        }
    }

    static class ClusterDataSet {
        private final Set<ClusterData> set = Sets.newHashSet();
        private volatile long lastAddTimestamp = System.currentTimeMillis();

        public boolean addAll(Collection<ClusterData> datas) {
            lastAddTimestamp = System.currentTimeMillis();
            return set.addAll(datas);
        }

        public Set<ClusterData> getSet() {
            return set;
        }

        public long getLastAddTimestamp() {
            return lastAddTimestamp;
        }
    }

    class TxHeartbeatServiceThread extends ServiceThread {

        @Override
        public String getServiceName() {
            return TxHeartbeatServiceThread.class.getName();
        }

        @Override
        public void run() {
            while (!this.isStopped()) {
                this.waitForRunning(TimeUnit.SECONDS.toMillis(ConfigurationManager.getProxyConfig().getTransactionHeartbeatPeriodSecond()));
            }
        }

        @Override
        protected void onWaitEnd() {
            scanProducerHeartBeat();
        }
    }

    @Override
    public void start() throws Exception {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        txHeartbeatServiceThread = new TxHeartbeatServiceThread();

        super.start();
        txHeartbeatServiceThread.start();
        heartbeatExecutors = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getTransactionHeartbeatThreadPoolNums(),
            proxyConfig.getTransactionHeartbeatThreadPoolNums(),
            0L, TimeUnit.MILLISECONDS,
            "TransactionHeartbeatRegisterThread",
            proxyConfig.getTransactionHeartbeatThreadPoolQueueCapacity()
        );
    }

    @Override
    public void shutdown() throws Exception {
        txHeartbeatServiceThread.shutdown();
        heartbeatExecutors.shutdown();
        super.shutdown();
    }
}
