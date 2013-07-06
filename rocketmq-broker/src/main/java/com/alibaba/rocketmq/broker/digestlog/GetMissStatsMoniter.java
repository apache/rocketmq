package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetMissStatsMoniter {
    static final Log log = LogFactory.getLog(GetMissStatsMoniter.class);
    private final ConcurrentHashMap<String/* topic,partition,cliHostName,group */,GetMissStatsInfo> getMissStatsInfos = new ConcurrentHashMap<String,GetMissStatsInfo> ();

    public void append(String topic, String partition, String cliHostName, String group,long count){
        GetMissStatsInfo getMissStatsInfo = getMissStatsInfos.get(topic+partition+cliHostName+group);
        if(null==getMissStatsInfo){
            getMissStatsInfo = new GetMissStatsInfo(topic,partition,cliHostName,group,count);
            getMissStatsInfos.put(topic+partition+cliHostName+group, getMissStatsInfo);
        }else{
            getMissStatsInfo.getCount().addAndGet(count);
        }
    }
    public void tolog(){
        for(GetMissStatsInfo getMissStatsInfo: getMissStatsInfos.values()){
            if(!getMissStatsInfo.isNull()){
                if(log.isInfoEnabled()){
                    log.info(getMissStatsInfo.tolog());
                }
                getMissStatsInfo.dispose();
            }
        }
    }

}
