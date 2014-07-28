package com.alibaba.rocketmq.tools.monitor;

import java.util.Map;

import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;


public class DefaultMonitorListener implements MonitorListener {

    public DefaultMonitorListener() {
    }


    @Override
    public void beginRound() {
    }


    @Override
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs) {
        System.out.println(undoneMsgs);
    }


    @Override
    public void reportFailedMsgs(FailedMsgs failedMsgs) {
        System.out.println(failedMsgs);
    }


    @Override
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent) {
        System.out.println(deleteMsgsEvent);
    }


    @Override
    public void reportConsumerRunningInfo(Map<String, ConsumerRunningInfo> criTable) {
    }


    @Override
    public void endRound() {
    }
}
