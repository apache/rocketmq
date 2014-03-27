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
package com.alibaba.rocketmq.broker.topic;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ConfigManager;
import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;


/**
 * Topic配置管理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author lansheng.zj@taobao.com
 * @since 2013-7-26
 */
public class TopicConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final long LockTimeoutMillis = 3000;
    private transient final Lock lockTopicConfigTable = new ReentrantLock();
    private transient BrokerController brokerController;

    // Topic配置
    private final ConcurrentHashMap<String, TopicConfig> topicConfigTable =
            new ConcurrentHashMap<String, TopicConfig>(1024);
    private final DataVersion dataVersion = new DataVersion();


    public TopicConfigManager() {
    }


    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            // MixAll.SELF_TEST_TOPIC
            TopicConfig topicConfig = new TopicConfig(MixAll.SELF_TEST_TOPIC);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.DEFAULT_TOPIC
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                TopicConfig topicConfig = new TopicConfig(MixAll.DEFAULT_TOPIC);
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // MixAll.BENCHMARK_TOPIC
            TopicConfig topicConfig = new TopicConfig(MixAll.BENCHMARK_TOPIC);
            topicConfig.setReadQueueNums(1024);
            topicConfig.setWriteQueueNums(1024);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 集群名字
            TopicConfig topicConfig =
                    new TopicConfig(this.brokerController.getBrokerConfig().getBrokerClusterName());
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // 服务器名字
            TopicConfig topicConfig =
                    new TopicConfig(this.brokerController.getBrokerConfig().getBrokerName());
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
    }


    public boolean isSystemTopic(final String topic) {
        boolean res = //
                topic.equals(MixAll.DEFAULT_TOPIC)//
                        || topic.equals(MixAll.SELF_TEST_TOPIC)//
                        || topic.equals(this.brokerController.getBrokerConfig().getBrokerClusterName())//
                        || topic.equals(ScheduleMessageService.SCHEDULE_TOPIC)//
                        || topic.equals(MixAll.SELF_TEST_TOPIC);

        return res;
    }


    public boolean isTopicCanSendMessage(final String topic) {
        boolean reservedWords =
                topic.equals(MixAll.DEFAULT_TOPIC)
                        || topic.equals(this.brokerController.getBrokerConfig().getBrokerClusterName());

        return !reservedWords;
    }


    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }


    /**
     * 发消息时，如果Topic不存在，尝试创建
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
            final String remoteAddress, final int clientDefaultTopicQueueNums) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                    if (defaultTopicConfig != null) {
                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(topic);

                            int queueNums =
                                    clientDefaultTopicQueueNums > defaultTopicConfig.getWriteQueueNums() ? defaultTopicConfig
                                        .getWriteQueueNums() : clientDefaultTopicQueueNums;

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        }
                        else {
                            log.warn("create new topic failed, because the default topic[" + defaultTopic
                                    + "] no perm, " + defaultTopicConfig.getPerm() + " producer: "
                                    + remoteAddress);
                        }
                    }
                    else {
                        log.warn("create new topic failed, because the default topic[" + defaultTopic
                                + "] not exist." + " producer: " + remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("create new topic by default topic[" + defaultTopic + "], " + topicConfig
                                + " producer: " + remoteAddress);

                        this.topicConfigTable.put(topic, topicConfig);

                        this.dataVersion.nextVersion();

                        createNew = true;

                        this.persist();
                    }
                }
                finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        }
        catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll();
        }

        return topicConfig;
    }


    public TopicConfig createTopicInSendMessageBackMethod(//
            final String topic, //
            final int clientDefaultTopicQueueNums,//
            final int perm//
    ) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                }
                finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        }
        catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll();
        }

        return topicConfig;
    }


    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old: " + old + " new: " + topicConfig);
        }
        else {
            log.info("create new topic, " + topicConfig);
        }

        this.dataVersion.nextVersion();

        this.brokerController.registerBrokerAll();

        this.persist();
    }


    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: " + old);
            this.dataVersion.nextVersion();
            this.persist();
        }
        else {
            log.warn("delete topic config failed, topic: " + topic + " not exist");
        }
    }


    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }


    @Override
    public String encode() {
        return encode(false);
    }


    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }


    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                    TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }


    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }


    @Override
    public String configFilePath() {
        return this.brokerController.getBrokerConfig().getTopicConfigPath();
    }


    public DataVersion getDataVersion() {
        return dataVersion;
    }


    public ConcurrentHashMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
}
