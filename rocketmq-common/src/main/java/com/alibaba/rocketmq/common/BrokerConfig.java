/**
 * $Id: BrokerConfig.java 1839 2013-05-16 02:12:02Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.alibaba.rocketmq.remoting.common.RemotingUtil;


/**
 * 服务器配置
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class BrokerConfig {
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY,
        System.getenv(MixAll.NAMESRV_ADDR_ENV));
    private String brokerIP1 = RemotingUtil.getLocalAddress();
    private String brokerIP2 = RemotingUtil.getLocalAddress();
    private String brokerName = localHostName();
    private String brokerClusterName = "C01";
    private long brokerId = MixAll.MASTER_ID;
    private int brokerPermission = MixAll.PERM_READ | MixAll.PERM_WRITE;
    private int defaultTopicQueueNums = 8;
    // 自动创建Topic功能是否开启（线上建议关闭）
    private boolean autoCreateTopicEnable = true;
    // 自动创建以集群名字命名的Topic功能是否开启
    private boolean clusterTopicEnable = true;
    // 自动创建订阅组功能是否开启（线上建议关闭）
    private boolean autoCreateSubscriptionGroup = true;

    private int sendMessageThreadPoolNums = Runtime.getRuntime().availableProcessors() * 6;
    private int pullMessageThreadPoolNums = Runtime.getRuntime().availableProcessors() * 6;
    private int adminBrokerThreadPoolNums = 8;

    private String topicConfigPath = System.getProperty("user.home") + File.separator + "store" + File.separator
            + "config" + File.separator + "topics.properties";

    private String consumerOffsetPath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "config" + File.separator + "consumeroffset.properties";

    private String consumerOffsetHistoryDir = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "config" + File.separator + "offsethistory";

    private int flushConsumerOffsetInterval = 1000 * 5;

    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    private String configFilePath = System.getProperty("user.home") + File.separator + "store" + File.separator
            + "config" + File.separator + "broker.properties";


    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return "DEFAULT_BROKER";
    }


    public String getRocketmqHome() {
        return rocketmqHome;
    }


    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public int getBrokerPermission() {
        return brokerPermission;
    }


    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }


    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }


    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }


    public void setAutoCreateTopicEnable(boolean autoCreateTopic) {
        this.autoCreateTopicEnable = autoCreateTopic;
    }


    public String getBrokerClusterName() {
        return brokerClusterName;
    }


    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }


    public String getBrokerIP1() {
        return brokerIP1;
    }


    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }


    public String getBrokerIP2() {
        return brokerIP2;
    }


    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }


    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }


    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }


    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }


    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }


    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }


    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }


    public String getTopicConfigPath() {
        return topicConfigPath;
    }


    public void setTopicConfigPath(String topicConfigPath) {
        this.topicConfigPath = topicConfigPath;
    }


    public String getConsumerOffsetPath() {
        return consumerOffsetPath;
    }


    public void setConsumerOffsetPath(String consumerOffsetPath) {
        this.consumerOffsetPath = consumerOffsetPath;
    }


    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }


    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }


    public String getConsumerOffsetHistoryDir() {
        return consumerOffsetHistoryDir;
    }


    public void setConsumerOffsetHistoryDir(String consumerOffsetHistoryDir) {
        this.consumerOffsetHistoryDir = consumerOffsetHistoryDir;
    }


    public int getFlushConsumerOffsetHistoryInterval() {
        return flushConsumerOffsetHistoryInterval;
    }


    public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
        this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
    }


    public String getConfigFilePath() {
        return configFilePath;
    }


    public void setConfigFilePath(String configFilePath) {
        this.configFilePath = configFilePath;
    }


    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }


    public void setClusterTopicEnable(boolean clusterTopicEnable) {
        this.clusterTopicEnable = clusterTopicEnable;
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }


    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }


    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }
}
