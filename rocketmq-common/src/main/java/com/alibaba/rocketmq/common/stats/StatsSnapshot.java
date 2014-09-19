package com.alibaba.rocketmq.common.stats;

public class StatsSnapshot {
    private long sum;
    private double tps;
    private double avgpt;


    public long getSum() {
        return sum;
    }


    public void setSum(long sum) {
        this.sum = sum;
    }


    public double getTps() {
        return tps;
    }


    public void setTps(double tps) {
        this.tps = tps;
    }


    public double getAvgpt() {
        return avgpt;
    }


    public void setAvgpt(double avgpt) {
        this.avgpt = avgpt;
    }
}
