/**
 * $Id: NamesrvConfig.java 1839 2013-05-16 02:12:02Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.namesrv;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;

import com.alibaba.rocketmq.common.MixAll;


/**
 * Name server 的配置类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author lansheng.zj@taobao.com
 * 
 */
public class NamesrvConfig {
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private String orderConfPath = System.getProperty("user.home") + File.separator + "namesrv"
            + File.separator + "orderConf.properties";
    private String brokerAddrConfPath = System.getProperty("user.home") + File.separator + "namesrv"
            + File.separator + "brokerAddr.properties";

    // namesrv 集群地址
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY,
        System.getenv(MixAll.NAMESRV_ADDR_ENV));
    // 同步namesrv信息的超时时间
    private long syncTimeout = 3000L;
    // 同步namesrv信息的时间间隔
    private long syncInterval = 30 * 1000L;
    // 一次spread操作最大等待时间
    private long groupWaitTimeout = 4000L;
    // 从broker上拉取配置信息超时时间
    private long pullFormBrokerTimeout = 3000L;
    // 从broker上拉取配置信息的时间间隔
    private long pullFormBrokerInterval = 30 * 1000L;
    // 从web server上获取namesrv地址列表的时间间隔
    private long addressInterval = 5 * 60 * 1000L;

    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);


    public void addPropertyChangeListener(final String propertyName, final PropertyChangeListener listener) {
        this.propertyChangeSupport.addPropertyChangeListener(propertyName, listener);
    }


    public long getPullFormBrokerInterval() {
        return pullFormBrokerInterval;
    }


    public void setPullFormBrokerInterval(long pullFormBrokerInterval) {
        this.pullFormBrokerInterval = pullFormBrokerInterval;
    }


    public long getAddressInterval() {
        return addressInterval;
    }


    public void setAddressInterval(long addressInterval) {
        this.addressInterval = addressInterval;
    }


    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        this.propertyChangeSupport.removePropertyChangeListener(listener);
    }


    public void firePropertyChange(String propertyName, Object oldValue, Object newValue) {
        propertyChangeSupport.firePropertyChange(propertyName, oldValue, newValue);
    }


    public long getSyncInterval() {
        return syncInterval;
    }


    public void setSyncInterval(long syncInterval) {
        this.syncInterval = syncInterval;
    }


    public long getSyncTimeout() {
        return syncTimeout;
    }


    public void setSyncTimeout(long syncTimeout) {
        this.syncTimeout = syncTimeout;
    }


    public String getRocketmqHome() {
        return rocketmqHome;
    }


    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public long getGroupWaitTimeout() {
        return groupWaitTimeout;
    }


    public void setGroupWaitTimeout(long groupWaitTimeout) {
        this.groupWaitTimeout = groupWaitTimeout;
    }


    public long getPullFormBrokerTimeout() {
        return pullFormBrokerTimeout;
    }


    public void setPullFormBrokerTimeout(long pullFormBrokerTimeout) {
        this.pullFormBrokerTimeout = pullFormBrokerTimeout;
    }


    public String getOrderConfPath() {
        return orderConfPath;
    }


    public void setOrderConfPath(String orderConfPath) {
        this.orderConfPath = orderConfPath;
    }


    public String getBrokerAddrConfPath() {
        return brokerAddrConfPath;
    }


    public void setBrokerAddrConfPath(String brokerAddrConfPath) {
        this.brokerAddrConfPath = brokerAddrConfPath;
    }

}
