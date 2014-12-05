package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


public class BrokerStatsData extends RemotingSerializable {
    // 最近一分钟内的统计
    private BrokerStatsItem statsMinute;
    // 最近一小时内的统计
    private BrokerStatsItem statsHour;
    // 最近一天内的的统计
    private BrokerStatsItem statsDay;


    public BrokerStatsItem getStatsMinute() {
        return statsMinute;
    }


    public void setStatsMinute(BrokerStatsItem statsMinute) {
        this.statsMinute = statsMinute;
    }


    public BrokerStatsItem getStatsHour() {
        return statsHour;
    }


    public void setStatsHour(BrokerStatsItem statsHour) {
        this.statsHour = statsHour;
    }


    public BrokerStatsItem getStatsDay() {
        return statsDay;
    }


    public void setStatsDay(BrokerStatsItem statsDay) {
        this.statsDay = statsDay;
    }
}
