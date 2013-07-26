/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * @since 2013-7-26
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


    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(config.getGroupName(), config);
        if (old != null) {
            log.info("update subscription group config, old: " + old + " new: " + config);
        }
        else {
            log.info("create new subscription group, " + config);
        }

        this.dataVersion.nextVersion();

        this.persist();
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