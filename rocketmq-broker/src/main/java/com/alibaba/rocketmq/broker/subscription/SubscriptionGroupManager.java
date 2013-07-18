package com.alibaba.rocketmq.broker.subscription;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ConfigManager;
import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 用来管理订阅组，包括订阅权限等
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SubscriptionGroupManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private transient BrokerController brokerController;

    // 订阅组
    private final ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
            new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);
    private final DataVersion dataVersion = new DataVersion();


    public SubscriptionGroupManager() {
    }


    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(group);
        if (null == subscriptionGroupConfig) {
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup()) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                this.subscriptionGroupTable.putIfAbsent(group, subscriptionGroupConfig);
                log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
                this.dataVersion.nextVersion();
                this.persist();
            }
        }

        return subscriptionGroupConfig;
    }


    @Override
    public String encode() {
        return this.encode(false);
    }


    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }


    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupManager obj =
                    RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
            }
        }
    }


    @Override
    public String configFilePath() {
        return this.brokerController.getBrokerConfig().getSubscriptionGroupPath();
    }


    public ConcurrentHashMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }


    public DataVersion getDataVersion() {
        return dataVersion;
    }
}