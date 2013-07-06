package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TransationStatsMoniter {
    static final Log log = LogFactory.getLog(TransationStatsMoniter.class);
    private ConcurrentHashMap<String/* topic,partition,cliHostName*/,TransationStatsInfo> TransationStatsInfos = new ConcurrentHashMap<String,TransationStatsInfo> ();

    public void append(String cliHostName,
                       long beginCount, long commitOneCount,long commitTwoCount,long forgetCount,
                       long rollbackCount,long recoverCount, long endCount,
                       long prepareCount){
        TransationStatsInfo transationStatsInfo = TransationStatsInfos.get(cliHostName);
        if(null==transationStatsInfo){
            transationStatsInfo = new TransationStatsInfo(cliHostName,beginCount, commitOneCount,commitTwoCount, forgetCount,rollbackCount,recoverCount, endCount,prepareCount);
            TransationStatsInfos.put(cliHostName, transationStatsInfo);
        }else{
            transationStatsInfo.getBeginCount().addAndGet(beginCount);
            transationStatsInfo.getCommitOneCount().addAndGet(commitOneCount);
            transationStatsInfo.getCommitTwoCount().addAndGet(commitTwoCount);
            transationStatsInfo.getForgetCount().addAndGet(forgetCount);
            transationStatsInfo.getRollbackCount().addAndGet(rollbackCount);
            transationStatsInfo.getRecoverCount().addAndGet(recoverCount);
            transationStatsInfo.getEndCount().addAndGet(endCount);
            transationStatsInfo.getPrepareCount().addAndGet(prepareCount);

        }
    }
    public void tolog(){
        for(TransationStatsInfo transationStatsInfo: TransationStatsInfos.values()){
            if(!transationStatsInfo.isNull()){
                if(log.isInfoEnabled()){
                    log.info(transationStatsInfo.tolog());
                }
                transationStatsInfo.dispose();
            }
        }
    }

}
