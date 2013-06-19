/**
 * $Id: TopicConfigManager.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.topic;

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
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author lansheng.zj@taobao.com
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

        // MixAll.DEFAULT_TOPIC
        TopicConfig topicConfig = new TopicConfig(MixAll.DEFAULT_TOPIC);
        topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
        topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
        int perm =
                this.brokerController.getBrokerConfig().isAutoCreateTopicEnable() ? PermName.PERM_INHERIT : 0;
        perm |= PermName.PERM_READ | PermName.PERM_WRITE;
        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // MixAll.SELF_TEST_TOPIC
        topicConfig = new TopicConfig(MixAll.SELF_TEST_TOPIC);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // 集群名字
        topicConfig = new TopicConfig(this.brokerController.getBrokerConfig().getBrokerClusterName());
        perm = PermName.PERM_INHERIT;
        if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
            perm |= PermName.PERM_READ | PermName.PERM_WRITE;
        }
        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
    }


    public String currentDataVersion() {
        return this.dataVersion.toJson();
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
        try {
            if (this.lockTopicConfigTable.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    TopicConfig topicConfig = this.topicConfigTable.get(topic);
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

                        this.persist();
                    }

                    return topicConfig;
                }
                finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        }
        catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        return null;
    }


    public TopicConfig createTopicInSendMessageBackMethod(//
            final String topic, //
            final int clientDefaultTopicQueueNums) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null)
            return topicConfig;

        try {
            if (this.lockTopicConfigTable.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);

                    if (topicConfig != null) {
                        log.info("create new topic {}", topicConfig);
                        this.topicConfigTable.put(topic, topicConfig);
                        this.persist();
                    }
                }
                finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        }
        catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
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

        this.persist();
    }


    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: " + old);
            this.persist();
        }
        else {
            log.warn("delete topic config failed, topic: " + topic + " not exist");
        }
    }


    @Override
    public String encode() {
        return RemotingSerializable.toJson(this);
    }


    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigManager obj = RemotingSerializable.fromJson(jsonString, TopicConfigManager.class);
            if (obj != null) {
                this.topicConfigTable.putAll(obj.topicConfigTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
            }
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
