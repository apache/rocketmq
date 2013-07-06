package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SlavePutStatsMoniter {
    static final Log log = LogFactory.getLog(SlavePutStatsMoniter.class);
    private ConcurrentHashMap<String/* topic,partition,cliHostName*/,PutStatsInfor> putStatsInfors = new ConcurrentHashMap<String,PutStatsInfor> ();

    public void append(String topic, String partition, String cliHostName,long succesCount, long faileCount, long timeoutCount, long excTime,long msgSize){
        PutStatsInfor putStatsInfor = putStatsInfors.get(topic+partition+cliHostName);
        if(null==putStatsInfor){
            putStatsInfor = new PutStatsInfor(topic,partition,cliHostName,succesCount,faileCount,timeoutCount,excTime,msgSize);
            putStatsInfors.put(topic+partition+cliHostName, putStatsInfor);
        }else{
            putStatsInfor.getSuccesCount().addAndGet(succesCount);
            putStatsInfor.getFaileCount().addAndGet(faileCount);
            putStatsInfor.getTimeoutCount().addAndGet(timeoutCount);
            putStatsInfor.getExcTime().addAndGet(excTime);
        }
        
    }
    public void tolog(){
        for(PutStatsInfor putStatsInfor: putStatsInfors.values()){
            if(!putStatsInfor.isNull()){
                if(log.isInfoEnabled()){
                    log.info(putStatsInfor.tolog());
                }
                putStatsInfor.dispose();
            }
        }
    }

}
