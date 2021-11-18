package org.apache.rocketmq.common.rpc;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClientMetadata {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    private final ConcurrentMap<String/* Topic */, ConcurrentMap<MessageQueue, String/*brokerName*/>> topicEndPointsTable = new ConcurrentHashMap<String, ConcurrentMap<MessageQueue, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
            new ConcurrentHashMap<String, HashMap<String, Integer>>();

    public void freshTopicRoute(String topic, TopicRouteData topicRouteData) {
        if (topic == null
            || topicRouteData == null) {
            return;
        }
        TopicRouteData old = this.topicRouteTable.get(topic);
        if (!topicRouteDataIsChange(old, topicRouteData)) {
            return ;
        }
        {
            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
            }
        }
        {
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
            if (mqEndPoints != null
                    && !mqEndPoints.isEmpty()) {
                topicEndPointsTable.put(topic, mqEndPoints);
            }
        }
    }


    public static boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = new TopicRouteData(olddata);
        TopicRouteData now = new TopicRouteData(nowdata);
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
        if (topicEndPointsTable.get(mq.getTopic()) != null
                && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
            return topicEndPointsTable.get(mq.getTopic()).get(mq);
        }
        return mq.getBrokerName();
    }

    public void refreshClusterInfo(ClusterInfo clusterInfo) {
        if (clusterInfo == null
            || clusterInfo.getBrokerAddrTable() == null) {
            return;
        }
        for (Map.Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
            brokerAddrTable.put(entry.getKey(), entry.getValue().getBrokerAddrs());
        }
    }

    public String findMasterBrokerAddr(String brokerName) {
        if (!brokerAddrTable.containsKey(brokerName)) {
            return null;
        }
        return brokerAddrTable.get(brokerName).get(MixAll.MASTER_ID);
    }

    public static ConcurrentMap<MessageQueue, String> topicRouteData2EndpointsForStaticTopic(final String topic, final TopicRouteData route) {
        if (route.getTopicQueueMappingByBroker() == null
                || route.getTopicQueueMappingByBroker().isEmpty()) {
            return new ConcurrentHashMap<MessageQueue, String>();
        }
        ConcurrentMap<MessageQueue, String> mqEndPoints = new ConcurrentHashMap<MessageQueue, String>();

        int totalNums = 0;
        for (Map.Entry<String, TopicQueueMappingInfo> entry : route.getTopicQueueMappingByBroker().entrySet()) {
            String brokerName = entry.getKey();
            //TODO check the epoch of
            if (entry.getValue().getTotalQueues() > totalNums) {
                if (totalNums != 0) {
                    log.warn("The static logic queue totalNums dose not match before {} {} != {}", topic, totalNums, entry.getValue().getTotalQueues());
                }
                totalNums = entry.getValue().getTotalQueues();
            }
            for (Map.Entry<Integer, Integer> idEntry : entry.getValue().getCurrIdMap().entrySet()) {
                int globalId = idEntry.getKey();
                MessageQueue mq = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, globalId);
                String oldBrokerName = mqEndPoints.put(mq, brokerName);
                log.warn("The static logic queue is duplicated {} {} {} ", mq, oldBrokerName, brokerName);
            }
        }
        //accomplish the static logic queues
        for (int i = 0; i < totalNums; i++) {
            MessageQueue mq = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, i);
            if (!mqEndPoints.containsKey(mq)) {
                mqEndPoints.put(mq, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
            }
        }
        return mqEndPoints;
    }

}
