/**
 * $Id: TopicConfigManager.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.topic;

import static com.alibaba.rocketmq.common.protocol.route.ObjectConverter.props2TopicConfigTable;
import io.netty.channel.ChannelHandlerContext;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.logger.LoggerName;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author lansheng.zj@taobao.com
 * 
 */
public class TopicConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    // Topic配置
    private final ConcurrentHashMap<String, TopicConfig> topicConfigTable =
            new ConcurrentHashMap<String, TopicConfig>(1024);
    private final Lock lockTopicConfigTable = new ReentrantLock();
    private static final long LockTimeoutMillis = 3000;

    private final DataVersion dataVersion = new DataVersion();

    private final BrokerController brokerController;


    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;

        // MixAll.DEFAULT_TOPIC
        TopicConfig topicConfig = new TopicConfig(MixAll.DEFAULT_TOPIC);
        topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
        topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
        int perm = this.brokerController.getBrokerConfig().isAutoCreateTopicEnable() ? MixAll.PERM_INHERIT : 0;
        perm |= MixAll.PERM_READ | MixAll.PERM_WRITE;
        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // MixAll.SELF_TEST_TOPIC
        topicConfig = new TopicConfig(MixAll.SELF_TEST_TOPIC);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // 集群名字
        topicConfig = new TopicConfig(this.brokerController.getBrokerConfig().getBrokerClusterName());
        perm = MixAll.PERM_INHERIT;
        if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
            perm |= MixAll.PERM_READ | MixAll.PERM_WRITE;
        }
        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
    }


    public boolean load() {
        try {
            String fileName = this.brokerController.getBrokerConfig().getTopicConfigPath();
            String content = MixAll.file2String(fileName);
            if (content != null) {
                Properties prop = MixAll.string2Properties(content);
                if (prop != null) {
                    return this.decode(prop);
                }
            }
        }
        catch (Exception e) {
        }

        return true;
    }


    public String getCurrentDataVersion() {
        return this.dataVersion.currentVersion();
    }


    public String encodeIncludeSysTopic() {
        return this.encode(true);
    }


    public String encodeNotIncludeSysTopic() {
        return this.encode(false);
    }


    private String encode(boolean includeSysTopic) {
        if (!this.topicConfigTable.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (TopicConfig config : this.topicConfigTable.values()) {
                if (!includeSysTopic) {
                    if (this.isSystemTopic(config.getTopicName()))
                        continue;
                }

                sb.append(config.getTopicName() + "=" + config.encode() + IOUtils.LINE_SEPARATOR);
            }

            return sb.toString();
        }

        return null;
    }


    private boolean decode(final Properties prop) {
        topicConfigTable.putAll(props2TopicConfigTable(prop, log));
        return true;
    }


    private boolean isSystemTopic(final String topic) {
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


    public synchronized void flush() {
        String content = this.encodeNotIncludeSysTopic();
        if (content != null) {
            String fileName = this.brokerController.getBrokerConfig().getTopicConfigPath();
            boolean result = MixAll.string2File(content, fileName);
            log.info("flush topic config, " + fileName + (result ? " OK" : " Failed"));
        }

        this.dataVersion.nextVersion();
    }


    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }


    /**
     * 发消息时，如果Topic不存在，尝试创建
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
            final ChannelHandlerContext ctx, final int clientDefaultTopicQueueNums) {
        final String remoteAddress = ctx != null ? ctx.channel().remoteAddress().toString() : "UNKNOW ADDR";

        try {
            if (this.lockTopicConfigTable.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null)
                    return topicConfig;

                TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                if (defaultTopicConfig != null) {
                    if (MixAll.isInherited(defaultTopicConfig.getPerm())) {
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
                        perm &= ~MixAll.PERM_INHERIT;
                        topicConfig.setPerm(perm);
                        topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                    }
                    else {
                        log.warn("create new topic failed, because the default topic[" + defaultTopic
                                + "] no perm, " + defaultTopicConfig.getPerm() + " producer: " + remoteAddress);
                    }
                }
                else {
                    log.warn("create new topic failed, because the default topic[" + defaultTopic + "] not exist."
                            + " producer: " + remoteAddress);
                }

                if (topicConfig != null) {
                    log.info("create new topic by default topic[" + defaultTopic + "], " + topicConfig
                            + " producer: " + remoteAddress);

                    this.topicConfigTable.put(topic, topicConfig);

                    this.flush();
                }

                return topicConfig;
            }
        }
        catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        return null;
    }


    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old: " + old + " new: " + topicConfig);
        }
        else {
            log.info("create new topic, " + topicConfig);
        }

        this.flush();
    }


    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: " + old);
            this.flush();
        }
        else {
            log.warn("delete topic config failed, topic: " + topic + " not exist");
        }
    }
}
