package com.alibaba.rocketmq.namesrv.routeinfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;


/**
 * 运行过程中的路由信息，数据只在内存，宕机后数据消失，但是Broker会定期推送最新数据
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-2
 */
public class RouteInfoManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    private final HashMap<String/* brokerAddr */, Long/* last update timestamp */> brokerLiveTable;
    private final HashMap<String/* brokerAddr */, DataVersion> topicVersionTable;


    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, Long>(256);
        this.topicVersionTable = new HashMap<String, DataVersion>(256);
    }


    /**
     * 判断Topic配置信息是否发生变更
     */
    private boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = this.topicVersionTable.get(brokerAddr);
        if (null == prev) {
            this.topicVersionTable.put(brokerAddr, dataVersion);
            return true;
        }
        else {
            if (!prev.equals(dataVersion)) {
                this.topicVersionTable.put(brokerAddr, dataVersion);
                return true;
            }
        }

        return false;
    }


    public void registerBroker(//
            final String clusterName,//
            final String brokerAddr,//
            final String brokerName,//
            final long brokerId,//
            final TopicConfigSerializeWrapper topicConfig//
    ) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                // 更新最后变更时间
                this.brokerLiveTable.put(brokerAddr, System.currentTimeMillis());

                // 更新集群信息
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (null == brokerNames) {
                    brokerNames = new HashSet<String>();
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }
                brokerNames.add(brokerName);

                // 更新主备信息
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    brokerData = new BrokerData();
                    brokerData.setBrokerName(brokerName);
                    HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
                    brokerData.setBrokerAddrs(brokerAddrs);

                    this.brokerAddrTable.put(brokerName, brokerData);
                }
                brokerData.getBrokerAddrs().put(brokerId, brokerAddr);

                // 更新Topic信息
                if (null != topicConfig //
                        && MixAll.MASTER_ID == brokerId) {
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfig.getDataVersion())) {

                    }
                }
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("registerBroker Exception", e);
        }
    }


    public void unregisterBroker() {

    }


    public TopicRouteData pickupTopicRouteData(final String topic) {
        return null;
    }
}
