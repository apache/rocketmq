package com.alibaba.rocketmq.filtersrv;

import com.alibaba.rocketmq.remoting.common.RemotingUtil;


public class FiltersrvConfig {
    // private String rocketmqHome =
    // System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
    // System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String rocketmqHome = "/Users/vive/Desktop/share/work/gitlab/rocketmq";
    private String namesrvAddr = "10.235.170.7:9876";

    // 连接到哪个Broker
    private String connectWhichBroker = "10.235.170.7:10911";
    // Filter Server对外服务的IP
    private String filterServerIP = RemotingUtil.getLocalAddress();
    private int compressMsgBodyOverHowmuch = 1024 * 4;
    private int zipCompressLevel = 5;


    public String getRocketmqHome() {
        return rocketmqHome;
    }


    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }


    public String getConnectWhichBroker() {
        return connectWhichBroker;
    }


    public void setConnectWhichBroker(String connectWhichBroker) {
        this.connectWhichBroker = connectWhichBroker;
    }


    public String getFilterServerIP() {
        return filterServerIP;
    }


    public void setFilterServerIP(String filterServerIP) {
        this.filterServerIP = filterServerIP;
    }


    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }


    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }


    public int getZipCompressLevel() {
        return zipCompressLevel;
    }


    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

}
