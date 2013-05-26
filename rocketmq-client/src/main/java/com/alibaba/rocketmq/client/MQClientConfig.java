/**
 * $Id: MQClientConfig.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client;

import java.io.File;

import com.alibaba.rocketmq.remoting.common.RemotingUtil;


/**
 * Producer与Consumer的公共配置
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MQClientConfig {
    private String namesrvAddr = null;
    private String logFileName = defaultClientLogFileName();
    private String logLevel = "INFO";
    private String clientIP = RemotingUtil.getLocalAddress();
    private String instanceName = "DEFAULT";
    private int clientCallbackExecutorThreads = 5;
    private int pollNameServerInteval = 1000 * 30;
    private int heartbeatBrokerInterval = 1000 * 30;
    private int uploadConsumerOffsetInterval = 1000 * 5;


    public static String defaultClientLogFileName() {
        return System.getProperty("user.home") + File.separator + "rocketmqlogs" + File.separator
                + "rocketmq_client.log";
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public String getLogFileName() {
        return logFileName;
    }


    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }


    public String getLogLevel() {
        return logLevel;
    }


    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }


    public String getClientIP() {
        return clientIP;
    }


    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }


    public String getInstanceName() {
        return instanceName;
    }


    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }


    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }


    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }


    public int getPollNameServerInteval() {
        return pollNameServerInteval;
    }


    public void setPollNameServerInteval(int pollNameServerInteval) {
        this.pollNameServerInteval = pollNameServerInteval;
    }


    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }


    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }


    public int getUploadConsumerOffsetInterval() {
        return uploadConsumerOffsetInterval;
    }


    public void setUploadConsumerOffsetInterval(int uploadConsumerOffsetInterval) {
        this.uploadConsumerOffsetInterval = uploadConsumerOffsetInterval;
    }


    @Override
    public String toString() {
        return "MQClientConfig [namesrvAddr=" + namesrvAddr + ", logFileName=" + logFileName + ", logLevel="
                + logLevel + ", clientIP=" + clientIP + ", instanceName=" + instanceName
                + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads + ", pollNameServerInteval="
                + pollNameServerInteval + ", heartbeatBrokerInterval=" + heartbeatBrokerInterval
                + ", uploadConsumerOffsetInterval=" + uploadConsumerOffsetInterval + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + clientCallbackExecutorThreads;
        result = prime * result + ((clientIP == null) ? 0 : clientIP.hashCode());
        result = prime * result + heartbeatBrokerInterval;
        result = prime * result + ((instanceName == null) ? 0 : instanceName.hashCode());
        result = prime * result + ((logFileName == null) ? 0 : logFileName.hashCode());
        result = prime * result + ((logLevel == null) ? 0 : logLevel.hashCode());
        result = prime * result + ((namesrvAddr == null) ? 0 : namesrvAddr.hashCode());
        result = prime * result + pollNameServerInteval;
        result = prime * result + uploadConsumerOffsetInterval;
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MQClientConfig other = (MQClientConfig) obj;
        if (clientCallbackExecutorThreads != other.clientCallbackExecutorThreads)
            return false;
        if (clientIP == null) {
            if (other.clientIP != null)
                return false;
        }
        else if (!clientIP.equals(other.clientIP))
            return false;
        if (heartbeatBrokerInterval != other.heartbeatBrokerInterval)
            return false;
        if (instanceName == null) {
            if (other.instanceName != null)
                return false;
        }
        else if (!instanceName.equals(other.instanceName))
            return false;
        if (logFileName == null) {
            if (other.logFileName != null)
                return false;
        }
        else if (!logFileName.equals(other.logFileName))
            return false;
        if (logLevel == null) {
            if (other.logLevel != null)
                return false;
        }
        else if (!logLevel.equals(other.logLevel))
            return false;
        if (namesrvAddr == null) {
            if (other.namesrvAddr != null)
                return false;
        }
        else if (!namesrvAddr.equals(other.namesrvAddr))
            return false;
        if (pollNameServerInteval != other.pollNameServerInteval)
            return false;
        if (uploadConsumerOffsetInterval != other.uploadConsumerOffsetInterval)
            return false;
        return true;
    }
}
