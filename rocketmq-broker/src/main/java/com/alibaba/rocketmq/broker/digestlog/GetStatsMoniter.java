package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class GetStatsMoniter{
    static final Log log = LogFactory.getLog(GetStatsMoniter.class);
    private ConcurrentHashMap<String/* topic,partition,cliHostName,group */,GetStatsInfo> getStatsInfos = new ConcurrentHashMap<String,GetStatsInfo> ();

    public void append(String topic, String partition, String cliHostName, String group,
                       long succesCount, long faileCount, long timeoutCount, long excTime,long msgSize){
        GetStatsInfo getStatsInfo = getStatsInfos.get(topic+partition+cliHostName+group);
        if(null==getStatsInfo){
            getStatsInfo = new GetStatsInfo(topic,partition,cliHostName,group,succesCount,faileCount,timeoutCount,excTime,msgSize);
            getStatsInfos.put(topic+partition+cliHostName+group, getStatsInfo);
        }else{
            getStatsInfo.getSuccesCount().addAndGet(succesCount);
            getStatsInfo.getFaileCount().addAndGet(faileCount);
            getStatsInfo.getTimeoutCount().addAndGet(timeoutCount);
            getStatsInfo.getExcTime().addAndGet(excTime);
            getStatsInfo.getMsgSizeSum().addAndGet(msgSize);
        }
    }
    public void tolog(){
        for(GetStatsInfo getStatsInfo: getStatsInfos.values()){
            if(!getStatsInfo.isNull()){
                if(log.isInfoEnabled()){
                    log.info(getStatsInfo.tolog());
                }
                getStatsInfo.dispose();
            }
        }
    }

    public ConcurrentHashMap<String, GetStatsInfo> getGetStatsInfos() {
        return getStatsInfos;
    }

    public void setGetStatsInfos(ConcurrentHashMap<String, GetStatsInfo> getStatsInfos) {
        this.getStatsInfos = getStatsInfos;
    }

}
