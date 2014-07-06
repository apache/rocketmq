package com.alibaba.rocketmq.tools.monitor;

import com.alibaba.rocketmq.common.MixAll;


public class MonitorConfig {
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY,
        System.getenv(MixAll.NAMESRV_ADDR_ENV));
    // 监控一轮间隔时间
    private int roundInterval = 1000 * 60;


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public int getRoundInterval() {
        return roundInterval;
    }


    public void setRoundInterval(int roundInterval) {
        this.roundInterval = roundInterval;
    }
}
