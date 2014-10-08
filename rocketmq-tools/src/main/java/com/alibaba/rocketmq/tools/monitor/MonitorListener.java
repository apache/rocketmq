package com.alibaba.rocketmq.tools.monitor;

import java.util.TreeMap;

import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;


/**
 * 监控监听器
 */
public interface MonitorListener {
    /**
     * 开始一轮监控
     */
    public void beginRound();


    /**
     * 汇报消息堆积情况
     */
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs);


    /**
     * 汇报消费失败情况
     */
    public void reportFailedMsgs(FailedMsgs failedMsgs);


    /**
     * 汇报消息删除情况
     */
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent);


    /**
     * 汇报Consumer内部运行数据结构
     */
    public void reportConsumerRunningInfo(TreeMap<String/* clientId */, ConsumerRunningInfo> criTable);


    /**
     * 结束一轮监控
     */
    public void endRound();
}
