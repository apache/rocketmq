package com.alibaba.rocketmq.filtersrv;

import com.alibaba.rocketmq.remoting.common.RemotingUtil;


public class FiltersrvConfig {
    // private String rocketmqHome =
    // System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
    // System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String rocketmqHome = "/Users/vive/Desktop/share/work/gitlab/rocketmq";

    // 连接到哪个Broker
    private String connectWhichBroker = "127.0.0.1:10911";
    // Filter Server对外服务的IP
    private String filterSrvIP = RemotingUtil.getLocalAddress();


    public String getRocketmqHome() {
        return rocketmqHome;
    }


    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }
}
