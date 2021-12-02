package org.apache.rocketmq.tools.admin;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.rpc.ClientMetadata;
import org.apache.rocketmq.common.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.common.statictopic.TopicRemappingDetailWrapper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MQAdminUtils {


    public static ClientMetadata getBrokerMetadata(DefaultMQAdminExt defaultMQAdminExt) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        ClientMetadata clientMetadata  = new ClientMetadata();
        refreshClusterInfo(defaultMQAdminExt, clientMetadata);
        return clientMetadata;
    }

    public static ClientMetadata getBrokerAndTopicMetadata(String topic, DefaultMQAdminExt defaultMQAdminExt) throws InterruptedException, RemotingException, MQBrokerException {
        ClientMetadata clientMetadata  = new ClientMetadata();
        refreshClusterInfo(defaultMQAdminExt, clientMetadata);
        refreshTopicRouteInfo(topic, defaultMQAdminExt, clientMetadata);
        return clientMetadata;
    }

    public static void refreshClusterInfo(DefaultMQAdminExt defaultMQAdminExt, ClientMetadata clientMetadata) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        ClusterInfo clusterInfo  = defaultMQAdminExt.examineBrokerClusterInfo();
        if (clusterInfo == null
                || clusterInfo.getClusterAddrTable().isEmpty()) {
            throw new RuntimeException("The Cluster info is empty");
        }
        clientMetadata.refreshClusterInfo(clusterInfo);
    }

    public static void refreshTopicRouteInfo(String topic, DefaultMQAdminExt defaultMQAdminExt, ClientMetadata clientMetadata) throws RemotingException, InterruptedException, MQBrokerException {
        TopicRouteData routeData = null;
        try {
            routeData = defaultMQAdminExt.examineTopicRouteInfo(topic);
        } catch (MQClientException exception) {
            if (exception.getResponseCode() != ResponseCode.TOPIC_NOT_EXIST) {
                throw new MQBrokerException(exception.getResponseCode(), exception.getErrorMessage());
            }
        }
        if (routeData != null
                && !routeData.getQueueDatas().isEmpty()) {
            clientMetadata.freshTopicRoute(topic, routeData);
        }
    }

    public static Set<String> getAllBrokersInSameCluster(Collection<String> brokers, DefaultMQAdminExt defaultMQAdminExt) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        ClusterInfo clusterInfo  = defaultMQAdminExt.examineBrokerClusterInfo();
        if (clusterInfo == null
                || clusterInfo.getClusterAddrTable().isEmpty()) {
            throw new RuntimeException("The Cluster info is empty");
        }
        Set<String> allBrokers = new HashSet<>();
        for (String broker: brokers) {
            if (allBrokers.contains(broker)) {
                continue;
            }
            for (Set<String> clusterBrokers : clusterInfo.getClusterAddrTable().values()) {
                if (clusterBrokers.contains(broker)) {
                    allBrokers.addAll(clusterBrokers);
                    break;
                }
            }
        }
        return allBrokers;
    }

    public static  void completeNoTargetBrokers(Map<String, TopicConfigAndQueueMapping> brokerConfigMap, DefaultMQAdminExt defaultMQAdminExt) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException {
        TopicConfigAndQueueMapping configMapping = brokerConfigMap.values().iterator().next();
        String topic = configMapping.getTopicName();
        int queueNum = configMapping.getMappingDetail().getTotalQueues();
        long newEpoch = configMapping.getMappingDetail().getEpoch();
        Set<String> allBrokers = getAllBrokersInSameCluster(brokerConfigMap.keySet(), defaultMQAdminExt);
        for (String broker: allBrokers) {
            if (!brokerConfigMap.containsKey(broker)) {
                brokerConfigMap.put(broker, new TopicConfigAndQueueMapping(new TopicConfig(topic, 0, 0), new TopicQueueMappingDetail(topic, queueNum, broker, newEpoch)));
            }
        }
    }

    public static void checkIfMasterAlive(Collection<String> brokers, DefaultMQAdminExt defaultMQAdminExt, ClientMetadata clientMetadata) {
        for (String broker : brokers) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            if (addr == null) {
                throw new RuntimeException("Can't find addr for broker " + broker);
            }
        }
    }

    public static void updateTopicConfigMappingAll(Map<String, TopicConfigAndQueueMapping> brokerConfigMap, DefaultMQAdminExt defaultMQAdminExt, boolean force) throws Exception {
        ClientMetadata clientMetadata = getBrokerMetadata(defaultMQAdminExt);
        checkIfMasterAlive(brokerConfigMap.keySet(), defaultMQAdminExt, clientMetadata);
        //If some succeed, and others fail, it will cause inconsistent data
        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            String broker = entry.getKey();
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }
    }

    public static void remappingStaticTopic(String topic, Set<String> brokersToMapIn, Set<String> brokersToMapOut, Map<String, TopicConfigAndQueueMapping> brokerConfigMap, int blockSeqSize, boolean force, DefaultMQAdminExt defaultMQAdminExt) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        ClientMetadata clientMetadata = MQAdminUtils.getBrokerMetadata(defaultMQAdminExt);
        MQAdminUtils.checkIfMasterAlive(brokerConfigMap.keySet(), defaultMQAdminExt, clientMetadata);
        // now do the remapping
        //Step1: let the new leader can be write without the logicOffset
        for (String broker: brokersToMapIn) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = brokerConfigMap.get(broker);
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }
        //Step2: forbid the write of old leader
        for (String broker: brokersToMapOut) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = brokerConfigMap.get(broker);
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }
        //Step3: decide the logic offset
        for (String broker: brokersToMapOut) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicStatsTable statsTable = defaultMQAdminExt.examineTopicStats(addr, topic);
            TopicConfigAndQueueMapping mapOutConfig = brokerConfigMap.get(broker);
            for (Map.Entry<Integer, List<LogicQueueMappingItem>> entry : mapOutConfig.getMappingDetail().getHostedQueues().entrySet()) {
                List<LogicQueueMappingItem> items = entry.getValue();
                Integer globalId = entry.getKey();
                if (items.size() < 2) {
                    continue;
                }
                LogicQueueMappingItem newLeader = items.get(items.size() - 1);
                LogicQueueMappingItem oldLeader = items.get(items.size() - 2);
                if (newLeader.getLogicOffset()  > 0) {
                    continue;
                }
                TopicOffset topicOffset = statsTable.getOffsetTable().get(new MessageQueue(topic, oldLeader.getBname(), oldLeader.getQueueId()));
                if (topicOffset == null) {
                    throw new RuntimeException("Cannot get the max offset for old leader " + oldLeader);
                }
                //TODO check the max offset, will it return -1?
                if (topicOffset.getMaxOffset() < oldLeader.getStartOffset()) {
                    throw new RuntimeException("The max offset is smaller then the start offset " + oldLeader + " " + topicOffset.getMaxOffset());
                }
                newLeader.setLogicOffset(TopicQueueMappingUtils.blockSeqRoundUp(oldLeader.computeStaticQueueOffset(topicOffset.getMaxOffset()), blockSeqSize));
                TopicConfigAndQueueMapping mapInConfig = brokerConfigMap.get(newLeader.getBname());
                //fresh the new leader
                TopicQueueMappingDetail.putMappingInfo(mapInConfig.getMappingDetail(), globalId, items);
            }
        }
        //Step4: write to the new leader with logic offset
        for (String broker: brokersToMapIn) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = brokerConfigMap.get(broker);
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }
        //Step5: write the non-target brokers
        for (String broker: brokerConfigMap.keySet()) {
            if (brokersToMapIn.contains(broker) || brokersToMapOut.contains(broker)) {
                continue;
            }
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = brokerConfigMap.get(broker);
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
        }
    }

    public static Map<String, TopicConfigAndQueueMapping> examineTopicConfigAll(String topic, DefaultMQAdminExt defaultMQAdminExt) throws RemotingException,  InterruptedException, MQBrokerException {
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        ClientMetadata clientMetadata = new ClientMetadata();
        boolean getFromBrokers = false;
        TopicRouteData routeData = null;
        try {
            routeData = defaultMQAdminExt.examineTopicRouteInfo(topic);
        } catch (MQClientException  exception) {
            if (exception.getResponseCode() != ResponseCode.TOPIC_NOT_EXIST) {
                throw new MQBrokerException(exception.getResponseCode(), exception.getErrorMessage());
            } else {
                getFromBrokers = true;
            }
        }
        if (!getFromBrokers) {
            if (routeData != null
                    && !routeData.getQueueDatas().isEmpty()) {
                clientMetadata.freshTopicRoute(topic, routeData);
                for (QueueData queueData: routeData.getQueueDatas()) {
                    String bname = queueData.getBrokerName();
                    String addr = clientMetadata.findMasterBrokerAddr(bname);
                    try {
                        TopicConfigAndQueueMapping mapping = (TopicConfigAndQueueMapping) defaultMQAdminExt.examineTopicConfig(addr, topic);
                        //allow the config is null
                        if (mapping != null) {
                            brokerConfigMap.put(bname, mapping);
                        }
                    } catch (MQBrokerException exception) {
                        if (exception.getResponseCode() != ResponseCode.TOPIC_NOT_EXIST) {
                            throw exception;
                        }
                    }

                }
            }
        } else {
            //if cannot get from nameserver, then check all the brokers
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            if (clusterInfo != null
                    && clusterInfo.getBrokerAddrTable() != null) {
                clientMetadata.refreshClusterInfo(clusterInfo);
            }
            for (Map.Entry<String, HashMap<Long, String>> entry : clientMetadata.getBrokerAddrTable().entrySet()) {
                String bname = entry.getKey();
                HashMap<Long, String> map = entry.getValue();
                String addr = map.get(MixAll.MASTER_ID);
                if (addr != null) {
                    try {
                        TopicConfigAndQueueMapping mapping = (TopicConfigAndQueueMapping) defaultMQAdminExt.examineTopicConfig(addr, topic);
                        //allow the config is null
                        if (mapping != null) {
                            brokerConfigMap.put(bname, mapping);
                        }
                    }  catch (MQBrokerException exception1) {
                        if (exception1.getResponseCode() != ResponseCode.TOPIC_NOT_EXIST) {
                            throw exception1;
                        }
                    }
                }
            }
        }
        return brokerConfigMap;
    }
}
