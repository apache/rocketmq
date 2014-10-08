package com.alibaba.rocketmq.tools.monitor;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;


public class DefaultMonitorListener implements MonitorListener {
    private final Logger log = ClientLogger.getLog();

    private final static String LogPrefix = "[MONITOR] ";

    private final static String LogNotify = LogPrefix + " [NOTIFY] ";


    public DefaultMonitorListener() {
    }


    @Override
    public void beginRound() {
        log.info(LogPrefix + "=========================================beginRound");
    }


    @Override
    public void reportUndoneMsgs(UndoneMsgs undoneMsgs) {
        log.info(String.format(LogPrefix + "reportUndoneMsgs: %s", undoneMsgs));
    }


    @Override
    public void reportFailedMsgs(FailedMsgs failedMsgs) {
        log.info(String.format(LogPrefix + "reportFailedMsgs: %s", failedMsgs));
    }


    @Override
    public void reportDeleteMsgsEvent(DeleteMsgsEvent deleteMsgsEvent) {
        log.info(String.format(LogPrefix + "reportDeleteMsgsEvent: %s", deleteMsgsEvent));
    }


    @Override
    public void reportConsumerRunningInfo(TreeMap<String, ConsumerRunningInfo> criTable) {
        // 分析订阅关系
        {
            boolean result = ConsumerRunningInfo.analyzeSubscription(criTable);
            if (!result) {
                log.info(String.format(LogNotify
                        + "reportConsumerRunningInfo: ConsumerGroup: %s, Subscription different", criTable
                    .firstEntry().getValue().getProperties().getProperty("consumerGroup")));
            }
        }

        // 分析顺序消息
        {
            Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, ConsumerRunningInfo> next = it.next();
                String result = ConsumerRunningInfo.analyzeProcessQueue(next.getKey(), next.getValue());
                if (result != null && !result.isEmpty()) {
                    log.info(String.format(LogNotify
                            + "reportConsumerRunningInfo: ConsumerGroup: %s, ClientId: %s, %s", //
                        criTable.firstEntry().getValue().getProperties().getProperty("consumerGroup"),//
                        next.getKey(),//
                        result));
                }
            }
        }
    }


    @Override
    public void endRound() {
        log.info(LogPrefix + "=========================================endRound");
    }
}
