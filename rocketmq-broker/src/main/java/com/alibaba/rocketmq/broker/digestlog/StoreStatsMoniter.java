package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.broker.BrokerController;


public class StoreStatsMoniter {
    // static final Log log = LogFactory.getLog(StoreStatsMoniter.class);
    private ConcurrentHashMap<String/* topic,partition,cliHostName */, StoreStatsInfo> storeStatsInfos =
            new ConcurrentHashMap<String, StoreStatsInfo>();
    private BrokerController brokerController;


    public StoreStatsMoniter(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public void putAppend(String topic, String partition, long putOffset) {
        StoreStatsInfo storeStatsInfo = storeStatsInfos.get(topic + partition);
        if (null == storeStatsInfo) {
            storeStatsInfo = new StoreStatsInfo(topic, partition, putOffset);
            storeStatsInfos.put(topic + partition, storeStatsInfo);
        }
        else if (putOffset > 0) {
            storeStatsInfo.getPutOffset().set(putOffset);
        }
    }


    public void getAppend(String topic, String partition, String group, long getOffset) {
        StoreStatsInfo storeStatsInfo = storeStatsInfos.get(topic + partition);
        if (null == storeStatsInfo) {
            storeStatsInfo = new StoreStatsInfo(topic, partition, 0);
            storeStatsInfos.put(topic + partition, storeStatsInfo);
        }
        else {
            if (getOffset > 0) {
                if (null == storeStatsInfo.getGetGroupinfo().get(group)) {
                    storeStatsInfo.getGetGroupinfo().put(group, new AtomicLong(getOffset));
                }
                else {
                    storeStatsInfo.getGetGroupinfo().get(group).set(getOffset);
                }
            }
        }
    }


    public void tolog() {
        for (StoreStatsInfo storeStatsInfo : storeStatsInfos.values()) {
            if (!storeStatsInfo.isNull()) {
                if (storeStatsInfo.getGetGroupinfo().size() > 0) {
                    storeStatsInfo.tolog();
                }
                else {
                    storeStatsInfo.toPutlog();
                }
                storeStatsInfo.dispose();
            }
        }
    }

}
