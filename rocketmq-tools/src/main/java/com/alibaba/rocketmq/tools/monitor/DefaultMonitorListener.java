package com.alibaba.rocketmq.tools.monitor;

import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;


public class DefaultMonitorListener implements MonitorListener {

    public DefaultMonitorListener() {
        // TODO Auto-generated constructor stub
    }


    @Override
    public void beginRound() {
        // TODO Auto-generated method stub

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
    public void reportConsumerRunningInfo(ConsumerRunningInfo consumerRunningInfo) {
        // TODO Auto-generated method stub

    }


    @Override
    public void endRound() {
        // TODO Auto-generated method stub
    }
}
