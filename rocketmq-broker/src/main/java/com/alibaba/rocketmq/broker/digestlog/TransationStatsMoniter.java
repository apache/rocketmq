package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TransationStatsMoniter {
    static final Log log = LogFactory.getLog(TransationStatsMoniter.class);
    private ConcurrentHashMap<String/* topic,partition,cliHostName */, TransationStatsInfo> TransationStatsInfos =
            new ConcurrentHashMap<String, TransationStatsInfo>();


    public void append(String topic,String partition,String cliHostName, long prepareCount, long commitCount, long rollbackCount) {
        TransationStatsInfo transationStatsInfo = TransationStatsInfos.get(cliHostName);
        if (null == transationStatsInfo) {
            transationStatsInfo =
                    new TransationStatsInfo(cliHostName, prepareCount, commitCount, rollbackCount);
            TransationStatsInfos.put(cliHostName, transationStatsInfo);
        }
        else {
            transationStatsInfo.getPrepareCount().addAndGet(prepareCount);
            transationStatsInfo.getCommitCount().addAndGet(commitCount);
            transationStatsInfo.getRollbackCount().addAndGet(rollbackCount);
        }
    }


    public void tolog() {
        for (TransationStatsInfo transationStatsInfo : TransationStatsInfos.values()) {
            if (!transationStatsInfo.isNull()) {
                if (log.isInfoEnabled()) {
                    log.info(transationStatsInfo.tolog());
                }
                transationStatsInfo.dispose();
            }
        }
    }

}
