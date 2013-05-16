/**
 * $Id$
 */
package com.alibaba.rocketmq.common.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.common.protocol.MetaProtos.BrokerDataPair;
import com.alibaba.rocketmq.common.protocol.MetaProtos.BrokerInfo;
import com.alibaba.rocketmq.common.protocol.MetaProtos.BrokerInfo.BrokerAddr;
import com.alibaba.rocketmq.common.protocol.MetaProtos.QueueInfo;
import com.alibaba.rocketmq.common.protocol.MetaProtos.TopicQueuePair;
import com.alibaba.rocketmq.common.protocol.MetaProtos.TopicRouteInfo;
import com.alibaba.rocketmq.common.protocol.MetaProtos.TopicRuntimeInfo;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.NVPair;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.NVPairList;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.StringList;


/**
 * @author lansheng.zj@taobao.com
 */
public class ObjectConverter {

    public static TopicRouteInfo topicRouteData2TopicRouteInfo(TopicRouteData topicRouteData) {
        TopicRouteInfo.Builder topicRouteInfo = TopicRouteInfo.newBuilder();
        for (QueueData queueData : topicRouteData.getQueueDatas()) {
            topicRouteInfo.addQueueInfos(queueData2QueueInfo(queueData));
        }

        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            topicRouteInfo.addBrokerInfos(brokerData2BrokerInfo(brokerData));
        }

        topicRouteInfo.setOrderTopicConf(topicRouteData.getOrderTopicConf());

        return topicRouteInfo.build();
    }


    public static TopicRouteData topicRouteInfo2TopicRouteData(TopicRouteInfo topicRouteInfo) {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<QueueInfo> queueInfoList = topicRouteInfo.getQueueInfosList();
        List<QueueData> queueDataList = new ArrayList<QueueData>();
        for (QueueInfo queueInfo : queueInfoList) {
            queueDataList.add(queueInfo2QueueData(queueInfo));
        }

        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        List<BrokerInfo> brokerInfoList = topicRouteInfo.getBrokerInfosList();
        for (BrokerInfo brokerInfo : brokerInfoList) {
            brokerDataList.add(brokerInfo2BrokerData(brokerInfo));
        }

        topicRouteData.setQueueDatas(queueDataList);
        topicRouteData.setBrokerDatas(brokerDataList);
        topicRouteData.setOrderTopicConf(topicRouteInfo.getOrderTopicConf());

        return topicRouteData;
    }


    public static QueueData queueInfo2QueueData(QueueInfo queueInfo) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(queueInfo.getBrokerName());
        queueData.setReadQueueNums(queueInfo.getReadQueueNums());
        queueData.setWriteQueueNums(queueInfo.getWriteQueueNums());
        queueData.setPerm(queueInfo.getPerm());

        return queueData;
    }


    public static QueueInfo queueData2QueueInfo(QueueData queueData) {
        QueueInfo.Builder qb = QueueInfo.newBuilder();
        qb.setBrokerName(queueData.getBrokerName());
        qb.setPerm(queueData.getPerm());
        qb.setReadQueueNums(queueData.getReadQueueNums());
        qb.setWriteQueueNums(queueData.getWriteQueueNums());

        return qb.build();
    }


    public static BrokerData brokerInfo2BrokerData(BrokerInfo brokerInfo) {
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(brokerInfo.getBrokerName());

        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        for (BrokerAddr brokerAddr : brokerInfo.getBrokerAddrsList()) {
            brokerAddrs.put(brokerAddr.getId(), brokerAddr.getAddr());
        }

        brokerData.setBrokerAddrs(brokerAddrs);

        return brokerData;
    }


    public static BrokerInfo brokerData2BrokerInfo(BrokerData brokerData) {
        BrokerInfo.Builder bb = BrokerInfo.newBuilder();
        bb.setBrokerName(brokerData.getBrokerName());
        for (Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
            BrokerAddr.Builder bab = BrokerAddr.newBuilder();
            bab.setId(entry.getKey());
            bab.setAddr(entry.getValue());
            bb.addBrokerAddrs(bab.build());
        }
        return bb.build();
    }


    public static Map<String, String> nvPairList2HashMap(NVPairList nvPairList) {
        HashMap<String, String> map = new HashMap<String, String>();
        for (RemotingProtos.NVPair nvPair : nvPairList.getFieldsList()) {
            map.put(nvPair.getName(), nvPair.getValue());
        }

        return map;
    }


    public static NVPairList hashMap2NVPairList(Map<String, String> map) {
        NVPairList.Builder nvlb = NVPairList.newBuilder();
        int index = 0;
        for (Entry<String, String> entry : map.entrySet()) {
            NVPair.Builder nvb = NVPair.newBuilder();
            nvb.setName(entry.getKey());
            nvb.setValue(entry.getValue());
            nvlb.addFields(index, nvb.build());
        }

        return nvlb.build();
    }


    public static TopicRuntimeData topicRuntimeInfo2TopicRuntimeData(TopicRuntimeInfo topicRuntimeInfo) {
        TopicRuntimeData topicRuntimeData = new TopicRuntimeData();
        for (TopicQueuePair topicQueuePair : topicRuntimeInfo.getTopicBrokersList()) {
            List<QueueInfo> listQueueInfo = topicQueuePair.getQueueInfoList();
            List<QueueData> listQueueData = new ArrayList<QueueData>();
            for (QueueInfo queueInfo : listQueueInfo) {
                listQueueData.add(queueInfo2QueueData(queueInfo));
            }
            topicRuntimeData.getTopicBrokers().put(topicQueuePair.getTopic(), listQueueData);
        }

        topicRuntimeData.setTopicOrderConfs(nvPairList2HashMap(topicRuntimeInfo.getTopicOrderConfs()));

        for (BrokerDataPair brokerDataPair : topicRuntimeInfo.getBrokersList()) {
            topicRuntimeData.getBrokers().put(brokerDataPair.getBrokerName(),
                brokerInfo2BrokerData(brokerDataPair.getBrokerInfo()));
        }

        topicRuntimeData.setBrokerList(topicRuntimeInfo.getBrokerList().getNameList());

        return topicRuntimeData;
    }


    public static StringList list2StringList(List<String> list) {
        StringList.Builder slb = StringList.newBuilder();
        for (String name : list) {
            slb.addName(name);
        }
        return slb.build();
    }


    public static TopicRuntimeInfo topicRuntimeData2TopicRuntimeInfo(TopicRuntimeData topicRuntimeData) {
        TopicRuntimeInfo.Builder tri = TopicRuntimeInfo.newBuilder();
        for (Entry<String, List<QueueData>> entry : topicRuntimeData.getTopicBrokers().entrySet()) {
            List<QueueData> queueDataList = entry.getValue();
            TopicQueuePair.Builder tqpb = TopicQueuePair.newBuilder();
            tqpb.setTopic(entry.getKey());
            for (QueueData queueData : queueDataList) {
                tqpb.addQueueInfo(queueData2QueueInfo(queueData));
            }
            tri.addTopicBrokers(tqpb.build());
        }

        tri.setTopicOrderConfs(hashMap2NVPairList(topicRuntimeData.getTopicOrderConfs()));

        for (Entry<String, BrokerData> entry : topicRuntimeData.getBrokers().entrySet()) {
            BrokerDataPair.Builder bdpb = BrokerDataPair.newBuilder();
            bdpb.setBrokerName(entry.getKey());
            bdpb.setBrokerInfo(brokerData2BrokerInfo(entry.getValue()));
            tri.addBrokers(bdpb.build());
        }

        tri.setBrokerList(list2StringList(topicRuntimeData.getBrokerList()));

        return tri.build();
    }


    public static <T> boolean equals(List<T> l1, List<T> l2) {
        if (l1 == l2)
            return true;
        if (null == l1 || null == l2)
            return false;
        if (l1.size() != l2.size())
            return false;

        for (T e : l1) {
            boolean equal = false;
            for (T a : l2) {
                equal = e.equals(a);
                if (equal)
                    break;
            }
            if (!equal)
                return false;
        }
        return true;
    }


    public static <K, V> boolean equals(Map<K, V> m1, Map<K, V> m2) {
        if (m1 == m2)
            return true;
        if (null == m1 || null == m2)
            return false;
        if (m1.size() != m2.size())
            return false;

        for (Entry<K, V> entry : m1.entrySet()) {
            V newV = m2.get(entry.getKey());
            if (!entry.getValue().equals(newV))
                return false;
        }

        return true;
    }


    public static Map<String, TopicConfig> props2TopicConfigTable(Properties prop, Logger log) {
        Map<String, TopicConfig> topicConfigTable = new HashMap<String, TopicConfig>();
        Set<Object> keyset = prop.keySet();
        for (Object object : keyset) {
            String propValue = prop.getProperty(object.toString());
            if (propValue != null) {
                TopicConfig tc = new TopicConfig(null);
                if (tc.decode(propValue)) {
                    topicConfigTable.put(tc.getTopicName(), tc);
                    if (null != log)
                        log.info("load topic config OK, " + tc);
                }
            }
        }

        return topicConfigTable;
    }

}
